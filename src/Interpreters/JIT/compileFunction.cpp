#include "compileFunction.h"

#if USE_EMBEDDED_COMPILER

#include <llvm/IR/BasicBlock.h>
#include <llvm/IR/Function.h>
#include <llvm/IR/IRBuilder.h>

#include <Common/Stopwatch.h>
#include <Common/ProfileEvents.h>
#include <DataTypes/Native.h>
#include <Interpreters/JIT/CHJIT.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeString.h>

namespace
{
    struct ColumnDataPlaceholder
    {
        llvm::Value * data_init = nullptr; /// first row
        llvm::Value * null_init = nullptr;
        llvm::Value * offset_init = nullptr;
        llvm::PHINode * data = nullptr; /// current row
        llvm::PHINode * null = nullptr;
        llvm::PHINode * offset = nullptr;
        /// for ColumnString
        llvm::Value * new_data = nullptr;
    };
}

namespace ProfileEvents
{
    extern const Event CompileFunction;
    extern const Event CompileExpressionsMicroseconds;
    extern const Event CompileExpressionsBytes;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

ColumnData getColumnData(const IColumn * column)
{
    ColumnData result;
    const bool is_const = isColumnConst(*column);

    if (is_const)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Input columns should not be constant");
    getColumnDataImpl(column, result);
    return result;
}

void getColumnDataImpl(const IColumn * column, ColumnData & column_data)
{
    if (const auto * nullable = typeid_cast<const ColumnNullable *>(column))
    {
        column_data.null_data = nullable->getNullMapColumn().getRawData().data;
        column = & nullable->getNestedColumn();
        getColumnDataImpl(column, column_data);
    }
    else if (const auto * column_string = typeid_cast<const ColumnString *>(column))
    {
        column_data.data = column_string->getChars().raw_data();
        /// handle PODArray pad_left
        column_data.offset_data = reinterpret_cast<const char *>(reinterpret_cast<const intptr_t>(column_string->getOffsets().raw_data()) - sizeof(ColumnString::Offset));
    }
    else
    {
        column_data.data = column->getRawData().data;
    }
}

static void compileFunction(llvm::Module & module, const IFunctionBase & function, JITContext & jit_context)
{
    /** Algorithm is to create a loop that iterate over ColumnDataRowsSize size_t argument and
     * over ColumnData data and null_data. On each step compiled expression from function
     * will be executed over column data and null_data row.
     *
     * Example of preudocode of generated instructions of function with 1 input column.
     * In case of multiple columns more column_i_data, column_i_null_data is created.
     *
     * void compiled_function(size_t rows_count, ColumnData * columns)
     * {
     *     /// Initialize column values
     *
     *     Column0Type * column_0_data = static_cast<Column0Type *>(columns[0].data);
     *     UInt8 * column_0_null_data = static_cast<UInt8>(columns[0].null_data);
     *
     *     /// Initialize other input columns data with indexes < input_columns_count
     *
     *     ResultType * result_column_data = static_cast<ResultType *>(columns[input_columns_count].data);
     *     UInt8 * result_column_null_data = static_cast<UInt8 *>(columns[input_columns_count].data);
     *
     *     if (rows_count == 0)
     *         goto end;
     *
     *     /// Loop
     *
     *     size_t counter = 0;
     *
     *     loop:
     *
     *     /// Create column values tuple in case of non nullable type it is just column value
     *     /// In case of nullable type it is tuple of column value and is column row nullable
     *
     *     Column0Tuple column_0_value;
     *     if (Column0Type is nullable)
     *     {
     *         value[0] = column_0_data;
     *         value[1] = static_cast<bool>(column_1_null_data);
     *     }
     *     else
     *     {
     *         value[0] = column_0_data
     *     }
     *
     *     /// Initialize other input column values tuple with indexes < input_columns_count
     *     /// execute_compiled_expressions function takes input columns values and must return single result value
     *
     *     if (ResultType is nullable)
     *     {
     *         (ResultType, bool) result_column_value = execute_compiled_expressions(column_0_value, ...);
     *         *result_column_data = result_column_value[0];
     *         *result_column_null_data = static_cast<UInt8>(result_column_value[1]);
     *     }
     *     else
     *     {
     *         ResultType result_column_value = execute_compiled_expressions(column_0_value, ...);
     *         *result_column_data = result_column_value;
     *     }
     *
     *     /// Increment input and result column current row pointer
     *
     *     ++column_0_data;
     *     if (Column 0 type is nullable)
     *     {
     *         ++column_0_null_data;
     *     }
     *
     *     ++result_column_data;
     *     if  (ResultType  is nullable)
     *     {
     *         ++result_column_null_data;
     *     }
     *
     *     /// Increment loop counter and check if we should exit.
     *
     *     ++counter;
     *     if (counter == rows_count)
     *         goto end;
     *     else
     *         goto loop;
     *
     *   /// End
     *   end:
     *       return;
     * }
     */

    ProfileEvents::increment(ProfileEvents::CompileFunction);

    const auto & arg_types = function.getArgumentTypes();

    llvm::IRBuilder<> b(module.getContext());
    auto * size_type = b.getIntNTy(sizeof(size_t) * 8);
    auto * data_type = llvm::StructType::get(b.getInt8PtrTy(), b.getInt8PtrTy(), b.getInt8PtrTy());
    auto * func_type = llvm::FunctionType::get(b.getVoidTy(), { size_type, data_type->getPointerTo() }, /*isVarArg=*/false);

    /// Declaration function
    jit_context.current_module = &module;
    auto * realloc_func_type = llvm::FunctionType::get(b.getInt8PtrTy(), {b.getInt8PtrTy(), b.getInt64Ty(), b.getInt64Ty()}, false);
    auto * realloc_func = llvm::Function::Create(realloc_func_type, llvm::Function::ExternalLinkage, "reallocForString", module);
    auto * memcpy_func_type = llvm::FunctionType::get(b.getInt8PtrTy(), { b.getInt8PtrTy(), b.getInt8PtrTy(), b.getInt64Ty()}, false);
    auto * memcpy_func = llvm::Function::Create(memcpy_func_type, llvm::Function::ExternalLinkage, "memcpy", module);
//    auto * printStr_func_type = llvm::FunctionType::get(b.getVoidTy(), { b.getInt8PtrTy(), b.getInt64Ty(), b.getInt64Ty()}, false);
//    auto * printStr_func = llvm::Function::Create(printStr_func_type, llvm::Function::ExternalLinkage, "printStr", module);
//    auto * printInt_func_type = llvm::FunctionType::get(b.getVoidTy(), { b.getInt64Ty() }, false);
//    auto * printInt_func = llvm::Function::Create(printInt_func_type, llvm::Function::ExternalLinkage, "printInt", module);
//    auto * printPtr_func_type = llvm::FunctionType::get(b.getVoidTy(), { b.getInt8PtrTy() }, false);
//    auto * printPtr_func = llvm::Function::Create(printPtr_func_type, llvm::Function::ExternalLinkage, "printPtr", module);
    auto * roundUpToPowerOfTwoOrZero_func_type = llvm::FunctionType::get(b.getInt64Ty(), { b.getInt64Ty()}, false);
    auto * roundUpToPowerOfTwoOrZero_func = llvm::Function::Create(roundUpToPowerOfTwoOrZero_func_type, llvm::Function::ExternalLinkage, "roundUpToPowerOfTwoOrZero", module);



    /// Create function in module

    auto * func = llvm::Function::Create(func_type, llvm::Function::ExternalLinkage, function.getName(), module);
    auto * args = func->args().begin();
    llvm::Value * rows_count_arg = args++;
    llvm::Value * columns_arg = args++;
    llvm::Value * result_column_ptr = nullptr;

    /// Initialize ColumnDataPlaceholder llvm representation of ColumnData

    auto * entry = llvm::BasicBlock::Create(b.getContext(), "entry", func);
    b.SetInsertPoint(entry);

    std::vector<ColumnDataPlaceholder> columns(arg_types.size() + 1);
    for (size_t i = 0; i <= arg_types.size(); ++i)
    {
        const auto & type = i == arg_types.size() ? function.getResultType() : arg_types[i];
        auto * data_ptr = b.CreateConstInBoundsGEP1_64(data_type, columns_arg, i);
        auto * data = b.CreateLoad(data_type, data_ptr);
        if (i == arg_types.size())
        {
            result_column_ptr = data_ptr;
        }
        columns[i].null_init = type->isNullable() ? b.CreateExtractValue(data, {1}) : nullptr;
        auto nested_type_ptr = removeNullable(type);
        WhichDataType which_data_type(nested_type_ptr);
        if (which_data_type.isString())
        {
            columns[i].data_init = b.CreatePointerCast(b.CreateExtractValue(data, {0}), b.getInt8Ty()->getPointerTo());
            columns[i].offset_init = b.CreatePointerCast(b.CreateExtractValue(data, {2}), b.getInt64Ty()->getPointerTo());
        }
        else
        {
            columns[i].data_init = b.CreatePointerCast(b.CreateExtractValue(data, {0}), toNativeType(b, removeNullable(type))->getPointerTo());
            columns[i].offset_init = nullptr;
        }
    }

    /// Initialize loop

    auto * end = llvm::BasicBlock::Create(b.getContext(), "end", func);
    auto * loop = llvm::BasicBlock::Create(b.getContext(), "loop", func);
    b.CreateCondBr(b.CreateICmpEQ(rows_count_arg, llvm::ConstantInt::get(size_type, 0)), end, loop);

    b.SetInsertPoint(loop);

    auto * counter_phi = b.CreatePHI(rows_count_arg->getType(), 2);
    counter_phi->addIncoming(llvm::ConstantInt::get(size_type, 0), entry);
    llvm::PHINode * string_capacity_phi = nullptr;
    if (columns.back().offset_init != nullptr)
    {
        string_capacity_phi = b.CreatePHI(b.getInt64Ty(), 2);
        string_capacity_phi->addIncoming(llvm::ConstantInt::get(b.getInt64Ty(), 0), entry);
    }


    for (auto & col : columns)
    {
        col.data = b.CreatePHI(col.data_init->getType(), 2);
        col.data->addIncoming(col.data_init, entry);
        if (col.null_init)
        {
            col.null = b.CreatePHI(col.null_init->getType(), 2);
            col.null->addIncoming(col.null_init, entry);
        }
        if (col.offset_init)
        {
            col.offset = b.CreatePHI(col.offset_init->getType(), 2);
            col.offset->addIncoming(col.offset_init, entry);
        }
    }

    /// Initialize column row values

    Values arguments;
    arguments.reserve(arg_types.size());

    for (size_t i = 0; i < arg_types.size(); ++i)
    {
        auto & column = columns[i];
        auto type = arg_types[i];


        /// create value
        llvm::Value * value = nullptr;
        auto nested_type_ptr = removeNullable(type);
        WhichDataType which_data_type(nested_type_ptr);
        if (which_data_type.isString())
        {
            auto * llvm_type = toNativeType(b, nested_type_ptr);
            value = llvm::Constant::getNullValue(llvm_type);
            auto * string_data = column.data;
            auto * string_offset = b.CreateLoad(b.getInt64Ty(), column.offset);
            auto * string_next_offset = b.CreateLoad(b.getInt64Ty(), b.CreateConstInBoundsGEP1_64(nullptr, column.offset, 1));
            value = b.CreateInsertValue(value, string_data, {0});
            value = b.CreateInsertValue(value, string_offset, {1});
            value = b.CreateInsertValue(value, string_next_offset, {2});
        }
        else
        {
            value = b.CreateLoad(toNativeType(b, removeNullable(type)), column.data);
        }

        /// check nullable
        if (!type->isNullable())
        {
            arguments.emplace_back(value);
            continue;
        }
        auto * is_null = b.CreateICmpNE(b.CreateLoad(b.getInt8Ty(), column.null), b.getInt8(0));
        auto * nullable_unitilized = llvm::Constant::getNullValue(toNativeType(b, type));
        auto * nullable_value = b.CreateInsertValue(b.CreateInsertValue(nullable_unitilized, value, {0}), is_null, {1});
        arguments.emplace_back(nullable_value);
    }

    /// Compile values for column rows and store compiled value in result column

    auto * result = function.compile(b, std::move(arguments), jit_context);
    llvm::Value * result_data = result;
    /// handle null
    auto * result_type = result_data->getType();
    /// check result is real nullable type
    /// because CAST(1, 'Nullable(UInt64)') may return UInt64 type not Nullable(UInt64) when not AccurateOrNullConvertStrategyAdditions
    /// it cause function.compile not return a nullable struct llvm type
    if (columns.back().null && result_type->isStructTy() && result_type->getStructName().startswith("Nullable"))
    {
        result_data = b.CreateExtractValue(result, {0});
        b.CreateStore(b.CreateSelect(b.CreateExtractValue(result, {1}), b.getInt8(1), b.getInt8(0)), columns.back().null);
    }
    /// handle data
    if (columns.back().offset)
    {
        llvm::Value * new_data = nullptr;
        /// all string result return a string with variable, so need realloc and memcpy
        auto * res_column_offset = b.CreateLoad(b.getInt64Ty(), columns.back().offset);
        auto * next_offset = b.CreateExtractValue(result_data, {2});
        auto * offset = b.CreateExtractValue(result_data, {1});
        auto * length = b.CreateSub(next_offset, offset);
        auto * data = b.CreateExtractValue(result_data, {0});
//        b.CreateCall(printInt_func, {counter_phi});
//        b.CreateCall(printStr_func, {data, offset, next_offset});
//        b.CreateCall(printInt_func, {res_column_offset});
//        b.CreateCall(printPtr_func, {columns.back().data});

        // call realloc
        // realloc(data_ptr, offset+length)

        auto * need_size = b.CreateAdd(res_column_offset, length);
        new_data = b.CreateCall(realloc_func, {columns.back().data, need_size, string_capacity_phi});
        auto * new_string_capacity = b.CreateCall(roundUpToPowerOfTwoOrZero_func, {need_size});
        auto * string_capacity = b.CreateSelect(b.CreateICmpUGT(string_capacity_phi, need_size), string_capacity_phi, new_string_capacity);
        string_capacity_phi->addIncoming(string_capacity, b.GetInsertBlock());
        // call memcpy
        // ptr = (int64)(start_ptr)+offset
        auto * last_data = b.CreateCast(llvm::Instruction::IntToPtr, b.CreateAdd(b.CreateCast(llvm::Instruction::PtrToInt, new_data, b.getInt64Ty()), res_column_offset), b.getInt8PtrTy());
        auto * last_data2 = b.CreateCast(llvm::Instruction::IntToPtr, b.CreateAdd(b.CreateCast(llvm::Instruction::PtrToInt, data, b.getInt64Ty()), offset), b.getInt8PtrTy());
        b.CreateCall(memcpy_func, {last_data, last_data2, length});
        columns.back().new_data = b.CreatePointerCast(new_data, b.getInt8PtrTy());

        /// store new data pointer to ColumnData.data
        auto * ptr = b.CreateInBoundsGEP(result_column_ptr, b.getInt64(0));
        b.CreateStore(columns.back().new_data, b.CreatePointerCast(ptr, b.getInt8PtrTy()->getPointerTo()));


        /// offsets start with -1, so only write next_offset
        b.CreateStore(need_size, b.CreateConstInBoundsGEP1_64(nullptr, columns.back().offset, 1));
    }
    else
    {
        b.CreateStore(result_data, columns.back().data);
    }

    /// End of loop

    auto * cur_block = b.GetInsertBlock();
    for (auto & col : columns)
    {
        if (!col.offset)
        {
            col.data->addIncoming(b.CreateConstInBoundsGEP1_64(nullptr, col.data, 1), cur_block);
        }
        else
        {
            if (col.new_data == nullptr)
            {
                /// for ColumnString args not move data ptr
                col.data->addIncoming(col.data_init, cur_block);
            }
            else
            {
                /// for ColumnString result, change data pointer
                col.data->addIncoming(col.new_data, cur_block);
            }
        }

        if (col.null)
            col.null->addIncoming(b.CreateConstInBoundsGEP1_64(nullptr, col.null, 1), cur_block);
        if (col.offset)
            col.offset->addIncoming(b.CreateConstInBoundsGEP1_64(nullptr, col.offset, 1), cur_block);
    }

    auto * value = b.CreateAdd(counter_phi, llvm::ConstantInt::get(size_type, 1));
    counter_phi->addIncoming(value, cur_block);

    b.CreateCondBr(b.CreateICmpEQ(value, rows_count_arg), end, loop);

    b.SetInsertPoint(end);
    b.CreateRetVoid();
}

CompiledFunction compileFunction(CHJIT & jit, const IFunctionBase & function)
{
    Stopwatch watch;
    std::shared_ptr<JITContext> jit_context = std::make_shared<JITContext>();
    auto compiled_module = jit.compileModule([&](llvm::Module & module)
    {
        compileFunction(module, function, *jit_context);
    });
    compiled_module.context = jit_context;

    ProfileEvents::increment(ProfileEvents::CompileExpressionsMicroseconds, watch.elapsedMicroseconds());
    ProfileEvents::increment(ProfileEvents::CompileExpressionsBytes, compiled_module.size);
    ProfileEvents::increment(ProfileEvents::CompileFunction);

    auto compiled_function_ptr = reinterpret_cast<JITCompiledFunction>(compiled_module.function_name_to_symbol[function.getName()]);
    assert(compiled_function_ptr);

    CompiledFunction result_compiled_function
    {
        .compiled_function = compiled_function_ptr,
        .compiled_module = compiled_module
    };

    return result_compiled_function;
}

static void compileCreateAggregateStatesFunctions(llvm::Module & module, const std::vector<AggregateFunctionWithOffset> & functions, const std::string & name)
{
    auto & context = module.getContext();
    llvm::IRBuilder<> b(context);

    auto * aggregate_data_places_type = b.getInt8Ty()->getPointerTo();
    auto * create_aggregate_states_function_type = llvm::FunctionType::get(b.getVoidTy(), { aggregate_data_places_type }, false);
    auto * create_aggregate_states_function = llvm::Function::Create(create_aggregate_states_function_type, llvm::Function::ExternalLinkage, name, module);

    auto * arguments = create_aggregate_states_function->args().begin();
    llvm::Value * aggregate_data_place_arg = arguments++;

    auto * entry = llvm::BasicBlock::Create(b.getContext(), "entry", create_aggregate_states_function);
    b.SetInsertPoint(entry);

    std::vector<ColumnDataPlaceholder> columns(functions.size());
    for (const auto & function_to_compile : functions)
    {
        size_t aggregate_function_offset = function_to_compile.aggregate_data_offset;
        const auto * aggregate_function = function_to_compile.function;
        auto * aggregation_place_with_offset = b.CreateConstInBoundsGEP1_64(nullptr, aggregate_data_place_arg, aggregate_function_offset);
        aggregate_function->compileCreate(b, aggregation_place_with_offset);
    }

    b.CreateRetVoid();
}

static void compileAddIntoAggregateStatesFunctions(llvm::Module & module, const std::vector<AggregateFunctionWithOffset> & functions, const std::string & name)
{
    auto & context = module.getContext();
    llvm::IRBuilder<> b(context);

    auto * size_type = b.getIntNTy(sizeof(size_t) * 8);
    auto * places_type = b.getInt8Ty()->getPointerTo()->getPointerTo();
    auto * column_data_type = llvm::StructType::get(b.getInt8PtrTy(), b.getInt8PtrTy(), b.getInt8PtrTy());

    auto * aggregate_loop_func_declaration = llvm::FunctionType::get(b.getVoidTy(), { size_type, column_data_type->getPointerTo(), places_type }, false);
    auto * aggregate_loop_func_definition = llvm::Function::Create(aggregate_loop_func_declaration, llvm::Function::ExternalLinkage, name, module);

    auto * arguments = aggregate_loop_func_definition->args().begin();
    llvm::Value * rows_count_arg = arguments++;
    llvm::Value * columns_arg = arguments++;
    llvm::Value * places_arg = arguments++;

    /// Initialize ColumnDataPlaceholder llvm representation of ColumnData

    auto * entry = llvm::BasicBlock::Create(b.getContext(), "entry", aggregate_loop_func_definition);
    b.SetInsertPoint(entry);

    std::vector<ColumnDataPlaceholder> columns;
    size_t previous_columns_size = 0;

    for (const auto & function : functions)
    {
        auto argument_types = function.function->getArgumentTypes();

        ColumnDataPlaceholder data_placeholder;

        size_t function_arguments_size = argument_types.size();

        for (size_t column_argument_index = 0; column_argument_index < function_arguments_size; ++column_argument_index)
        {
            const auto & argument_type = argument_types[column_argument_index];
            auto * data = b.CreateLoad(column_data_type, b.CreateConstInBoundsGEP1_64(column_data_type, columns_arg, previous_columns_size + column_argument_index));
            data_placeholder.data_init = b.CreatePointerCast(b.CreateExtractValue(data, {0}), toNativeType(b, removeNullable(argument_type))->getPointerTo());
            data_placeholder.null_init = argument_type->isNullable() ? b.CreateExtractValue(data, {1}) : nullptr;
            columns.emplace_back(data_placeholder);
        }

        previous_columns_size += function_arguments_size;
    }

    /// Initialize loop

    auto * end = llvm::BasicBlock::Create(b.getContext(), "end", aggregate_loop_func_definition);
    auto * loop = llvm::BasicBlock::Create(b.getContext(), "loop", aggregate_loop_func_definition);

    b.CreateCondBr(b.CreateICmpEQ(rows_count_arg, llvm::ConstantInt::get(size_type, 0)), end, loop);

    b.SetInsertPoint(loop);

    auto * counter_phi = b.CreatePHI(rows_count_arg->getType(), 2);
    counter_phi->addIncoming(llvm::ConstantInt::get(size_type, 0), entry);

    auto * places_phi = b.CreatePHI(places_arg->getType(), 2);
    places_phi->addIncoming(places_arg, entry);

    for (auto & col : columns)
    {
        col.data = b.CreatePHI(col.data_init->getType(), 2);
        col.data->addIncoming(col.data_init, entry);

        if (col.null_init)
        {
            col.null = b.CreatePHI(col.null_init->getType(), 2);
            col.null->addIncoming(col.null_init, entry);
        }
    }

    auto * aggregation_place = b.CreateLoad(b.getInt8Ty()->getPointerTo(), places_phi);

    previous_columns_size = 0;
    for (const auto & function : functions)
    {
        size_t aggregate_function_offset = function.aggregate_data_offset;
        const auto * aggregate_function_ptr = function.function;

        auto arguments_types = function.function->getArgumentTypes();
        std::vector<llvm::Value *> arguments_values;

        size_t function_arguments_size = arguments_types.size();
        arguments_values.resize(function_arguments_size);

        for (size_t column_argument_index = 0; column_argument_index < function_arguments_size; ++column_argument_index)
        {
            auto * column_argument_data = columns[previous_columns_size + column_argument_index].data;
            auto * column_argument_null_data = columns[previous_columns_size + column_argument_index].null;

            auto & argument_type = arguments_types[column_argument_index];

            auto * value = b.CreateLoad(toNativeType(b, removeNullable(argument_type)), column_argument_data);
            if (!argument_type->isNullable())
            {
                arguments_values[column_argument_index] = value;
                continue;
            }

            auto * is_null = b.CreateICmpNE(b.CreateLoad(b.getInt8Ty(), column_argument_null_data), b.getInt8(0));
            auto * nullable_unitilized = llvm::Constant::getNullValue(toNativeType(b, argument_type));
            auto * nullable_value = b.CreateInsertValue(b.CreateInsertValue(nullable_unitilized, value, {0}), is_null, {1});
            arguments_values[column_argument_index] = nullable_value;
        }

        auto * aggregation_place_with_offset = b.CreateConstInBoundsGEP1_64(nullptr, aggregation_place, aggregate_function_offset);
        aggregate_function_ptr->compileAdd(b, aggregation_place_with_offset, arguments_types, arguments_values);

        previous_columns_size += function_arguments_size;
    }

    /// End of loop

    auto * cur_block = b.GetInsertBlock();
    for (auto & col : columns)
    {
        col.data->addIncoming(b.CreateConstInBoundsGEP1_64(nullptr, col.data, 1), cur_block);

        if (col.null)
            col.null->addIncoming(b.CreateConstInBoundsGEP1_64(nullptr, col.null, 1), cur_block);
    }

    places_phi->addIncoming(b.CreateConstInBoundsGEP1_64(nullptr, places_phi, 1), cur_block);

    auto * value = b.CreateAdd(counter_phi, llvm::ConstantInt::get(size_type, 1));
    counter_phi->addIncoming(value, cur_block);

    b.CreateCondBr(b.CreateICmpEQ(value, rows_count_arg), end, loop);

    b.SetInsertPoint(end);
    b.CreateRetVoid();
}

static void compileMergeAggregatesStates(llvm::Module & module, const std::vector<AggregateFunctionWithOffset> & functions, const std::string & name)
{
    auto & context = module.getContext();
    llvm::IRBuilder<> b(context);

    auto * aggregate_data_places_type = b.getInt8Ty()->getPointerTo();
    auto * aggregate_loop_func_declaration = llvm::FunctionType::get(b.getVoidTy(), { aggregate_data_places_type, aggregate_data_places_type }, false);
    auto * aggregate_loop_func = llvm::Function::Create(aggregate_loop_func_declaration, llvm::Function::ExternalLinkage, name, module);

    auto * arguments = aggregate_loop_func->args().begin();
    llvm::Value * aggregate_data_place_dst_arg = arguments++;
    llvm::Value * aggregate_data_place_src_arg = arguments++;

    auto * entry = llvm::BasicBlock::Create(b.getContext(), "entry", aggregate_loop_func);
    b.SetInsertPoint(entry);

    for (const auto & function_to_compile : functions)
    {
        size_t aggregate_function_offset = function_to_compile.aggregate_data_offset;
        const auto * aggregate_function_ptr = function_to_compile.function;

        auto * aggregate_data_place_merge_dst_with_offset = b.CreateConstInBoundsGEP1_64(nullptr, aggregate_data_place_dst_arg, aggregate_function_offset);
        auto * aggregate_data_place_merge_src_with_offset = b.CreateConstInBoundsGEP1_64(nullptr, aggregate_data_place_src_arg, aggregate_function_offset);

        aggregate_function_ptr->compileMerge(b, aggregate_data_place_merge_dst_with_offset, aggregate_data_place_merge_src_with_offset);
    }

    b.CreateRetVoid();
}

static void compileInsertAggregatesIntoResultColumns(llvm::Module & module, const std::vector<AggregateFunctionWithOffset> & functions, const std::string & name)
{
    auto & context = module.getContext();
    llvm::IRBuilder<> b(context);

    auto * size_type = b.getIntNTy(sizeof(size_t) * 8);

    auto * column_data_type = llvm::StructType::get(b.getInt8PtrTy(), b.getInt8PtrTy(), b.getInt8PtrTy());
    auto * aggregate_data_places_type = b.getInt8Ty()->getPointerTo()->getPointerTo();
    auto * aggregate_loop_func_declaration = llvm::FunctionType::get(b.getVoidTy(), { size_type, column_data_type->getPointerTo(), aggregate_data_places_type }, false);
    auto * aggregate_loop_func = llvm::Function::Create(aggregate_loop_func_declaration, llvm::Function::ExternalLinkage, name, module);

    auto * arguments = aggregate_loop_func->args().begin();
    llvm::Value * rows_count_arg = &*arguments++;
    llvm::Value * columns_arg = &*arguments++;
    llvm::Value * aggregate_data_places_arg = &*arguments++;

    auto * entry = llvm::BasicBlock::Create(b.getContext(), "entry", aggregate_loop_func);
    b.SetInsertPoint(entry);

    std::vector<ColumnDataPlaceholder> columns(functions.size());
    for (size_t i = 0; i < functions.size(); ++i)
    {
        auto return_type = functions[i].function->getReturnType();
        auto * data = b.CreateLoad(column_data_type, b.CreateConstInBoundsGEP1_64(column_data_type, columns_arg, i));
        columns[i].data_init = b.CreatePointerCast(b.CreateExtractValue(data, {0}), toNativeType(b, removeNullable(return_type))->getPointerTo());
        columns[i].null_init = return_type->isNullable() ? b.CreateExtractValue(data, {1}) : nullptr;
    }

    auto * end = llvm::BasicBlock::Create(b.getContext(), "end", aggregate_loop_func);
    auto * loop = llvm::BasicBlock::Create(b.getContext(), "loop", aggregate_loop_func);

    b.CreateCondBr(b.CreateICmpEQ(rows_count_arg, llvm::ConstantInt::get(size_type, 0)), end, loop);

    b.SetInsertPoint(loop);

    auto * counter_phi = b.CreatePHI(rows_count_arg->getType(), 2);
    counter_phi->addIncoming(llvm::ConstantInt::get(size_type, 0), entry);

    auto * aggregate_data_place_phi = b.CreatePHI(aggregate_data_places_type, 2);
    aggregate_data_place_phi->addIncoming(aggregate_data_places_arg, entry);

    for (auto & col : columns)
    {
        col.data = b.CreatePHI(col.data_init->getType(), 2);
        col.data->addIncoming(col.data_init, entry);

        if (col.null_init)
        {
            col.null = b.CreatePHI(col.null_init->getType(), 2);
            col.null->addIncoming(col.null_init, entry);
        }
    }

    for (size_t i = 0; i < functions.size(); ++i)
    {
        size_t aggregate_function_offset = functions[i].aggregate_data_offset;
        const auto * aggregate_function_ptr = functions[i].function;

        auto * aggregate_data_place = b.CreateLoad(b.getInt8Ty()->getPointerTo(), aggregate_data_place_phi);
        auto * aggregation_place_with_offset = b.CreateConstInBoundsGEP1_64(nullptr, aggregate_data_place, aggregate_function_offset);

        auto * final_value = aggregate_function_ptr->compileGetResult(b, aggregation_place_with_offset);

        if (columns[i].null_init)
        {
            b.CreateStore(b.CreateExtractValue(final_value, {0}), columns[i].data);
            b.CreateStore(b.CreateSelect(b.CreateExtractValue(final_value, {1}), b.getInt8(1), b.getInt8(0)), columns[i].null);
        }
        else
        {
            b.CreateStore(final_value, columns[i].data);
        }
    }

    /// End of loop

    auto * cur_block = b.GetInsertBlock();
    for (auto & col : columns)
    {
        col.data->addIncoming(b.CreateConstInBoundsGEP1_64(nullptr, col.data, 1), cur_block);

        if (col.null)
            col.null->addIncoming(b.CreateConstInBoundsGEP1_64(nullptr, col.null, 1), cur_block);
    }

    auto * value = b.CreateAdd(counter_phi, llvm::ConstantInt::get(size_type, 1), "", true, true);
    counter_phi->addIncoming(value, cur_block);

    aggregate_data_place_phi->addIncoming(b.CreateConstInBoundsGEP1_64(nullptr, aggregate_data_place_phi, 1), cur_block);

    b.CreateCondBr(b.CreateICmpEQ(value, rows_count_arg), end, loop);

    b.SetInsertPoint(end);
    b.CreateRetVoid();
}

CompiledAggregateFunctions compileAggregateFunctons(CHJIT & jit, const std::vector<AggregateFunctionWithOffset> & functions, std::string functions_dump_name)
{
    std::string create_aggregate_states_functions_name = functions_dump_name + "_create";
    std::string add_aggregate_states_functions_name = functions_dump_name + "_add";
    std::string merge_aggregate_states_functions_name = functions_dump_name + "_merge";
    std::string insert_aggregate_states_functions_name = functions_dump_name + "_insert";

    auto compiled_module = jit.compileModule([&](llvm::Module & module)
    {
        compileCreateAggregateStatesFunctions(module, functions, create_aggregate_states_functions_name);
        compileAddIntoAggregateStatesFunctions(module, functions, add_aggregate_states_functions_name);
        compileMergeAggregatesStates(module, functions, merge_aggregate_states_functions_name);
        compileInsertAggregatesIntoResultColumns(module, functions, insert_aggregate_states_functions_name);
    });

    auto create_aggregate_states_function = reinterpret_cast<JITCreateAggregateStatesFunction>(compiled_module.function_name_to_symbol[create_aggregate_states_functions_name]);
    auto add_into_aggregate_states_function = reinterpret_cast<JITAddIntoAggregateStatesFunction>(compiled_module.function_name_to_symbol[add_aggregate_states_functions_name]);
    auto merge_aggregate_states_function = reinterpret_cast<JITMergeAggregateStatesFunction>(compiled_module.function_name_to_symbol[merge_aggregate_states_functions_name]);
    auto insert_aggregate_states_function = reinterpret_cast<JITInsertAggregateStatesIntoColumnsFunction>(compiled_module.function_name_to_symbol[insert_aggregate_states_functions_name]);

    assert(create_aggregate_states_function);
    assert(add_into_aggregate_states_function);
    assert(merge_aggregate_states_function);
    assert(insert_aggregate_states_function);

    CompiledAggregateFunctions compiled_aggregate_functions
    {
        .create_aggregate_states_function = create_aggregate_states_function,
        .add_into_aggregate_states_function = add_into_aggregate_states_function,
        .merge_aggregate_states_function = merge_aggregate_states_function,
        .insert_aggregates_into_columns_function = insert_aggregate_states_function,

        .functions_count = functions.size(),
        .compiled_module = std::move(compiled_module)
    };

    return compiled_aggregate_functions;
}

}

#endif
