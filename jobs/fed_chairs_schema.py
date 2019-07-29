from pyspark.sql.types import StructType, StructField, BooleanType, DecimalType, StringType, DateType, ArrayType

SCHEMA_FED_CHAIR = StructType([
    StructField("first_name", StringType(), False),
    StructField("last_name", StringType(), False),
    StructField("start_date", DateType(), False),
    StructField("end_date", DateType(), True),
    StructField("meetings", ArrayType(StructType([
        StructField("date", DateType(), False),
        StructField("current_fed_funds", StructType([
            StructField("upper", DecimalType(6, 4), False),
            StructField("lower", DecimalType(6, 4), False)]),
                    False),
        StructField("new_fed_funds", StructType([
            StructField("upper", DecimalType(6, 4), False),
            StructField("lower", DecimalType(6, 4), False)]), False),
        StructField("rate_change", BooleanType(), False)]),
        False), False)
])