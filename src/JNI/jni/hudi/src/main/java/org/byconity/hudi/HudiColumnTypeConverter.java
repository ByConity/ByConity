package org.byconity.hudi;

import avro.shaded.com.google.common.base.Preconditions;
import org.apache.hudi.org.apache.avro.LogicalTypes;
import org.apache.hudi.org.apache.avro.LogicalType;
import org.apache.hudi.org.apache.avro.Schema;

import java.util.List;
import java.util.stream.Collectors;

public class HudiColumnTypeConverter {
    // used for HUDI MOR reader only
    // convert hudi column type(avroSchema) to hive type string with some customization
    public static String fromHudiTypeToHiveTypeString(Schema avroSchema) {
        Schema.Type columnType = avroSchema.getType();
        LogicalType logicalType = avroSchema.getLogicalType();

        switch (columnType) {
            case BOOLEAN:
                return "boolean";
            case INT:
                if (logicalType instanceof LogicalTypes.Date) {
                    return "date";
                } else if (logicalType instanceof LogicalTypes.TimeMillis) {
                    throw new UnsupportedOperationException(String.format("Unsupported hudi %s type of column %s", avroSchema.getType().getName(), avroSchema.getName()));
                } else {
                    return "int";
                }
            case LONG:
                if (logicalType instanceof LogicalTypes.TimeMicros) {
                    throw new UnsupportedOperationException(String.format("Unsupported hudi %s type of column %s", avroSchema.getType().getName(), avroSchema.getName()));
                } else if (logicalType instanceof LogicalTypes.TimestampMillis
                        || logicalType instanceof LogicalTypes.TimestampMicros) {
                    // customized value for int64 based timestamp
                    return logicalType.getName();
                } else {
                    return "bigint";
                }
            case FLOAT:
                return "float";
            case DOUBLE:
                return "double";
            case STRING:
                return "string";
            case ARRAY:
                String elementType = fromHudiTypeToHiveTypeString(avroSchema.getElementType());
                return String.format("array<%s>", elementType);
            case FIXED:
            case BYTES:
                if (logicalType instanceof LogicalTypes.Decimal) {
                    int precision = ((LogicalTypes.Decimal) logicalType).getPrecision();
                    int scale = ((LogicalTypes.Decimal) logicalType).getScale();
                    return String.format("decimal(%s,%s)", precision, scale);
                } else {
                    return "string";
                }
            case RECORD:
                // Struct type
                List<Schema.Field> fields = avroSchema.getFields();
                Preconditions.checkArgument(fields.size() > 0);
                String nameToType = fields.stream()
                        .map(f -> String.format("%s:%s", f.name(),
                                fromHudiTypeToHiveTypeString(f.schema())))
                        .collect(Collectors.joining(","));
                return String.format("struct<%s>", nameToType);
            case MAP:
                Schema value = avroSchema.getValueType();
                String valueType = fromHudiTypeToHiveTypeString(value);
                return String.format("map<%s,%s>", "string", valueType);
            case UNION:
                List<Schema> nonNullMembers = avroSchema.getTypes().stream()
                        .filter(schema -> !Schema.Type.NULL.equals(schema.getType()))
                        .collect(Collectors.toList());
                return fromHudiTypeToHiveTypeString(nonNullMembers.get(0));
            case ENUM:
            default:
                throw new UnsupportedOperationException(String.format("Unsupported hudi %s type of column %s", avroSchema.getType().getName(), avroSchema.getName()));
        }
    }
}
