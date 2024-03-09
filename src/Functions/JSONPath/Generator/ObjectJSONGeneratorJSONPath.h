#pragma once

#include <Functions/JSONPath/Generator/IObjectJSONGenerator.h>
#include <Functions/JSONPath/Generator/ObjectJSONVisitorJSONPathMemberAccess.h>
#include <Functions/JSONPath/Generator/ObjectJSONVisitorJSONPathRange.h>
#include <Functions/JSONPath/Generator/ObjectJSONVisitorJSONPathRoot.h>
#include <Functions/JSONPath/Generator/ObjectJSONVisitorJSONPathStar.h>
#include <Functions/JSONPath/Generator/VisitorStatus.h>

#include <Functions/JSONPath/ASTs/ASTJSONPath.h>
#include <Functions/JSONPath/Generator/IGenerator_fwd.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

class ObjectJSONGeneratorJSONPath : public IObjectJSONGenerator
{
public:
    /**
     * Traverses children ASTs of ASTJSONPathQuery and creates a vector of corresponding visitors
     * @param query_ptr_ pointer to ASTJSONPathQuery
     */
    explicit ObjectJSONGeneratorJSONPath(ASTPtr query_ptr_)
    {
        query_ptr = query_ptr_;
        const auto * path = query_ptr->as<ASTJSONPath>();
        if (!path)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid path");
        }
        const auto * query = path->jsonpath_query;

        for (auto child_ast : query->children)
        {
            if (typeid_cast<ASTJSONPathRoot *>(child_ast.get()))
            {
                visitors.push_back(std::make_shared<ObjectJSONVisitorJSONPathRoot>(child_ast));
            }
            else if (typeid_cast<ASTJSONPathMemberAccess *>(child_ast.get()))
            {
                visitors.push_back(std::make_shared<ObjectJSONVisitorJSONPathMemberAccess>(child_ast));
            }
            else if (typeid_cast<ASTJSONPathRange *>(child_ast.get()))
            {
                visitors.push_back(std::make_shared<ObjectJSONVisitorJSONPathRange>(child_ast));
            }
            else if (typeid_cast<ASTJSONPathStar *>(child_ast.get()))
            {
                visitors.push_back(std::make_shared<ObjectJSONVisitorJSONPathStar>(child_ast));
            }
        }
    }

    const char * getName() const override { return "GeneratorJSONPath"; }

    /**
     * This method exposes API of traversing all paths, described by JSONPath,
     *  to SQLJSON Functions.
     * Expected usage is to iteratively call this method from inside the function
     *  and to execute custom logic with received element or handle an error.
     * On each such call getNextItem will yield next item into element argument
     *  and modify its internal state to prepare for next call.
     *
     * @param iterator root of object json
     * @return is the generator exhausted
     */
    VisitorStatus getNextItem(ObjectIterator & iterator) override
    {
        while (true)
        {
            /// element passed to us actually is root, so here we assign current to root
            if (current_visitor < 0)
            {
                return VisitorStatus::Exhausted;
            }

            for (int i = 0; i < current_visitor; ++i)
            {
                visitors[i]->apply(iterator);
            }

            VisitorStatus status = VisitorStatus::Error;
            for (size_t i = current_visitor; i < visitors.size(); ++i)
            {
                status = visitors[i]->visit(iterator);
                current_visitor = static_cast<int>(i);
                if (status == VisitorStatus::Error || status == VisitorStatus::Ignore)
                {
                    break;
                }
            }
            updateVisitorsForNextRun();

            if (status != VisitorStatus::Ignore)
            {
                return status;
            }
        }
    }

private:
    bool updateVisitorsForNextRun()
    {
        while (current_visitor >= 0 && visitors[current_visitor]->isExhausted())
        {
            visitors[current_visitor]->reinitialize();
            current_visitor--;
        }
        if (current_visitor >= 0)
        {
            visitors[current_visitor]->updateState();
        }
        return current_visitor >= 0;
    }

    int current_visitor = 0;
    ASTPtr query_ptr;
    ObjectJSONVisitorList visitors;
};

}
