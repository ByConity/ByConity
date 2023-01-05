#pragma once

#include <Parsers/IAST.h>

class WriteBuffer;
class ReadBuffer;

namespace DB
{

ASTPtr createByASTType(ASTType type, ReadBuffer & buf);

void serializeAST(const ASTPtr & ast, WriteBuffer & buf);

void serializeAST(const IAST & ast, WriteBuffer & buf);

ASTPtr deserializeAST(ReadBuffer & buf);

void serializeASTs(const ASTs & asts, WriteBuffer & buf);

ASTs deserializeASTs(ReadBuffer & buf);

ASTPtr deserializeASTWithChildren(ASTs & children, ReadBuffer & buf);

}
