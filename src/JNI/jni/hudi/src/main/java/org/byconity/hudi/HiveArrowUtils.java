package org.byconity.hudi;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class HiveArrowUtils {
    public static Schema hiveToArrowSchema(StructField[] structFields) {
        List<Field> fields = new ArrayList<>();
        List<String> fieldNames = Arrays.stream(structFields)
                .map(StructField::getFieldName).collect(Collectors.toList());
        List<ObjectInspector> objectInspectors = Arrays.stream(structFields)
                .map(StructField::getFieldObjectInspector).collect(Collectors.toList());

        for (int i = 0; i < fieldNames.size(); ++i) {
            Field field = hiveColumnToArrowField(fieldNames.get(i), objectInspectors.get(i));
            fields.add(field);
        }
        return new Schema(fields);
    }

    public static Field hiveColumnToArrowField(String fieldName, ObjectInspector fieldObjectInspector) {
        switch (fieldObjectInspector.getCategory()) {
            case PRIMITIVE:
                PrimitiveObjectInspector primitiveInspector = (PrimitiveObjectInspector) fieldObjectInspector;
                switch (primitiveInspector.getPrimitiveCategory())
                {
                    case BOOLEAN:
                        return Field.nullable(fieldName, Types.MinorType.BIT.getType());
                    case BYTE:
                        return Field.nullable(fieldName, Types.MinorType.TINYINT.getType());
                    case SHORT:
                        return Field.nullable(fieldName, Types.MinorType.SMALLINT.getType());
                    case INT:
                        return Field.nullable(fieldName, Types.MinorType.INT.getType());
                    case LONG:
                        return Field.nullable(fieldName, Types.MinorType.BIGINT.getType());
                    case FLOAT:
                        return Field.nullable(fieldName, Types.MinorType.FLOAT4.getType());
                    case DOUBLE:
                        return Field.nullable(fieldName, Types.MinorType.FLOAT8.getType());
                    case STRING:
                    case VARCHAR:
                    case CHAR:
                        return Field.nullable(fieldName, Types.MinorType.VARCHAR.getType());
                    default:
                        throw new IllegalArgumentException();
                }
            case LIST:
            case STRUCT:
            case UNION :
            default:
                throw new IllegalArgumentException();
        }
    }

    public static void setArrowFieldValue(FieldVector fieldVector, ObjectInspector fieldObjectInspector, int row_idx, Object raw)
    {
        if (fieldObjectInspector.getCategory() == ObjectInspector.Category.PRIMITIVE) {
            PrimitiveObjectInspector primitiveObjectInspector = (PrimitiveObjectInspector) fieldObjectInspector;
            Object data = primitiveObjectInspector.getPrimitiveJavaObject(raw);
            switch (primitiveObjectInspector.getPrimitiveCategory())
            {
                case BOOLEAN:
                    ((BitVector) fieldVector).setSafe(row_idx, (int) data);
                    break;
                case BYTE:
                case SHORT:
                    ((SmallIntVector) fieldVector).setSafe(row_idx, (short) data);
                    break;
                case INT:
                    ((IntVector) fieldVector).setSafe(row_idx, (int) data);
                    break;
                case LONG:
                    ((BigIntVector) fieldVector).setSafe(row_idx, (long) data);
                    break;
                case FLOAT:
                    ((Float4Vector) fieldVector).setSafe(row_idx, (float) data);
                    break;
                case DOUBLE:
                    ((Float8Vector) fieldVector).setSafe(row_idx, (double) data);
                    break;
                case STRING:
                case CHAR:
                case VARCHAR:
                    ((VarCharVector) fieldVector).setSafe(row_idx, ((String) data).getBytes(StandardCharsets.UTF_8));
                    break;
                case BINARY:
                    ((VarBinaryVector) fieldVector).setSafe(row_idx, (byte[]) data);
                    break;
                default:
                    throw new IllegalArgumentException();
            }

        } else {
            throw new IllegalArgumentException();
        }
    }
}
