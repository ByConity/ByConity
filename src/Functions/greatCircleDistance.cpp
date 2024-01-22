#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnConst.h>
#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionFactory.h>
#include <Common/TargetSpecific.h>
#include <Functions/PerformanceAdaptors.h>
#include <common/range.h>
#include <cmath>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

/** Calculates the distance between two geographical locations.
  * There are three variants:
  * greatCircleAngle: calculates the distance on a sphere in degrees: https://en.wikipedia.org/wiki/Great-circle_distance
  * greatCircleDistance: calculates the distance on a sphere in meters.
  * geoDistance: calculates the distance on WGS-84 ellipsoid in meters.
  *
  * The function calculates distance between two points on Earth specified by longitude and latitude in degrees.
  *
  * Latitude must be in [-90, 90], longitude must be [-180, 180].
  *
  * Original code of this implementation of this function is here:
  * https://github.com/sphinxsearch/sphinx/blob/409f2c2b5b2ff70b04e38f92b6b1a890326bad65/src/sphinxexpr.cpp#L3825.
  * Andrey Aksenov, the author of original code, permitted to use this code in ClickHouse under the Apache 2.0 license.
  * Presentation about this code from Highload++ Siberia 2019 is here https://github.com/ClickHouse/ClickHouse/files/3324740/1_._._GEODIST_._.pdf
  * The main idea of this implementation is optimisations based on Taylor series, trigonometric identity
  *  and calculated constants once for cosine, arcsine(sqrt) and look up table.
  */

namespace
{

constexpr double PI = 3.14159265358979323846;
constexpr float RAD_IN_DEG = static_cast<float>(PI / 180.0);
constexpr float RAD_IN_DEG_HALF = static_cast<float>(PI / 360.0);

constexpr size_t COS_LUT_SIZE = 1024; // maxerr 0.00063%
constexpr size_t ASIN_SQRT_LUT_SIZE = 512;
constexpr size_t METRIC_LUT_SIZE = 1024;

/** Earth radius in meters using WGS84 authalic radius.
  * We use this value to be consistent with H3 library.
  */
constexpr float EARTH_RADIUS = 6371007.180918475;
constexpr float EARTH_DIAMETER = 2 * EARTH_RADIUS;


float cos_lut[COS_LUT_SIZE + 1];       /// cos(x) table
float asin_sqrt_lut[ASIN_SQRT_LUT_SIZE + 1]; /// asin(sqrt(x)) * earth_diameter table

float sphere_metric_lut[METRIC_LUT_SIZE + 1]; /// sphere metric, unitless: the distance in degrees for one degree across longitude depending on latitude
float sphere_metric_meters_lut[METRIC_LUT_SIZE + 1]; /// sphere metric: the distance in meters for one degree across longitude depending on latitude
float wgs84_metric_meters_lut[2 * (METRIC_LUT_SIZE + 1)]; /// ellipsoid metric: the distance in meters across one degree latitude/longitude depending on latitude


inline double sqr(double v)
{
    return v * v;
}

inline float sqrf(float v)
{
    return v * v;
}

void geodistInit()
{
    for (size_t i = 0; i <= COS_LUT_SIZE; ++i)
        cos_lut[i] = static_cast<float>(cos(2 * PI * i / COS_LUT_SIZE)); // [0, 2 * pi] -> [0, COS_LUT_SIZE]

    for (size_t i = 0; i <= ASIN_SQRT_LUT_SIZE; ++i)
        asin_sqrt_lut[i] = static_cast<float>(asin(
            sqrt(static_cast<double>(i) / ASIN_SQRT_LUT_SIZE))); // [0, 1] -> [0, ASIN_SQRT_LUT_SIZE]

    for (size_t i = 0; i <= METRIC_LUT_SIZE; ++i)
    {
        double latitude = i * (PI / METRIC_LUT_SIZE) - PI * 0.5; // [-pi / 2, pi / 2] -> [0, METRIC_LUT_SIZE]

        /// Squared metric coefficients (for the distance in meters) on a tangent plane, for latitude and longitude (in degrees),
        /// depending on the latitude (in radians).

        /// https://github.com/mapbox/cheap-ruler/blob/master/index.js#L67
        wgs84_metric_meters_lut[i * 2] = static_cast<float>(sqr(111132.09 - 566.05 * cos(2 * latitude) + 1.20 * cos(4 * latitude)));
        wgs84_metric_meters_lut[i * 2 + 1] = static_cast<float>(sqr(111415.13 * cos(latitude) - 94.55 * cos(3 * latitude) + 0.12 * cos(5 * latitude)));

        sphere_metric_meters_lut[i] = static_cast<float>(sqr((EARTH_DIAMETER * PI / 360) * cos(latitude)));

        sphere_metric_lut[i] = sqrf(cosf(latitude));
    }
}

inline NO_SANITIZE_UNDEFINED size_t floatToIndex(float x)
{
    /// Implementation specific behaviour on overflow or infinite value.
    return static_cast<size_t>(x);
}

inline float geodistDegDiff(float f)
{
    f = fabsf(f);
    if (f > 180)
        f = 360 - f;
    return f;
}

inline float geodistFastCos(float x)
{
    float y = fabsf(x) * (COS_LUT_SIZE / PI / 2);
    size_t i = floatToIndex(y);
    y -= i;
    i &= (COS_LUT_SIZE - 1);
    return cos_lut[i] + (cos_lut[i + 1] - cos_lut[i]) * y;
}

inline float geodistFastSin(float x)
{
    float y = fabsf(x) * (COS_LUT_SIZE / PI / 2);
    size_t i = floatToIndex(y);
    y -= i;
    i = (i - COS_LUT_SIZE / 4) & (COS_LUT_SIZE - 1); // cos(x - pi / 2) = sin(x), costable / 4 = pi / 2
    return cos_lut[i] + (cos_lut[i + 1] - cos_lut[i]) * y;
}

/// fast implementation of asin(sqrt(x))
/// max error in floats 0.00369%, in doubles 0.00072%
inline float geodistFastAsinSqrt(float x)
{
    if (x < 0.122f)
    {
        // distance under 4546 km, Taylor error under 0.00072%
        float y = sqrtf(x);
        return y + x * y * 0.166666666666666f + x * x * y * 0.075f + x * x * x * y * 0.044642857142857f;
    }
    if (x < 0.948f)
    {
        // distance under 17083 km, 512-entry LUT error under 0.00072%
        x *= ASIN_SQRT_LUT_SIZE;
        size_t i = floatToIndex(x);
        return asin_sqrt_lut[i] + (asin_sqrt_lut[i + 1] - asin_sqrt_lut[i]) * (x - i);
    }
    return asinf(sqrtf(x)); // distance over 17083 km, just compute exact
}


enum class Method
{
    SPHERE_DEGREES,
    SPHERE_METERS,
    WGS84_METERS,
};

}

DECLARE_MULTITARGET_CODE(

namespace
{

template <Method method>
float distance(float lon1deg, float lat1deg, float lon2deg, float lat2deg)
{
    float lat_diff = geodistDegDiff(lat1deg - lat2deg);
    float lon_diff = geodistDegDiff(lon1deg - lon2deg);

    if (lon_diff < 13)
    {
        // points are close enough; use flat ellipsoid model
        // interpolate metric coefficients using latitudes midpoint

        /// Why comparing only difference in longitude?
        /// If longitudes are different enough, there is a big difference between great circle line and a line with constant latitude.
        ///  (Remember how a plane flies from Moscow to New York)
        /// But if longitude is close but latitude is different enough, there is no difference between meridian and great circle line.

        float latitude_midpoint = (lat1deg + lat2deg + 180) * METRIC_LUT_SIZE / 360; // [-90, 90] degrees -> [0, METRIC_LUT_SIZE] indexes
        size_t latitude_midpoint_index = floatToIndex(latitude_midpoint) & (METRIC_LUT_SIZE - 1);

        /// This is linear interpolation between two table items at index "latitude_midpoint_index" and "latitude_midpoint_index + 1".

        float k_lat{};
        float k_lon{};

        if constexpr (method == Method::SPHERE_DEGREES)
        {
            k_lat = 1;

            k_lon = sphere_metric_lut[latitude_midpoint_index]
                + (sphere_metric_lut[latitude_midpoint_index + 1] - sphere_metric_lut[latitude_midpoint_index]) * (latitude_midpoint - latitude_midpoint_index);
        }
        else if constexpr (method == Method::SPHERE_METERS)
        {
            k_lat = sqr(EARTH_DIAMETER * PI / 360);

            k_lon = sphere_metric_meters_lut[latitude_midpoint_index]
                + (sphere_metric_meters_lut[latitude_midpoint_index + 1] - sphere_metric_meters_lut[latitude_midpoint_index]) * (latitude_midpoint - latitude_midpoint_index);
        }
        else if constexpr (method == Method::WGS84_METERS)
        {
            k_lat = wgs84_metric_meters_lut[latitude_midpoint_index * 2]
                + (wgs84_metric_meters_lut[(latitude_midpoint_index + 1) * 2] - wgs84_metric_meters_lut[latitude_midpoint_index * 2]) * (latitude_midpoint - latitude_midpoint_index);

            k_lon = wgs84_metric_meters_lut[latitude_midpoint_index * 2 + 1]
                + (wgs84_metric_meters_lut[(latitude_midpoint_index + 1) * 2 + 1] - wgs84_metric_meters_lut[latitude_midpoint_index * 2 + 1]) * (latitude_midpoint - latitude_midpoint_index);
        }

        /// Metric on a tangent plane: it differs from Euclidean metric only by scale of coordinates.
        return sqrtf(k_lat * lat_diff * lat_diff + k_lon * lon_diff * lon_diff);
    }
    else
    {
        // points too far away; use haversine

        float a = sqrf(geodistFastSin(lat_diff * RAD_IN_DEG_HALF))
            + geodistFastCos(lat1deg * RAD_IN_DEG) * geodistFastCos(lat2deg * RAD_IN_DEG) * sqrf(geodistFastSin(lon_diff * RAD_IN_DEG_HALF));

        if constexpr (method == Method::SPHERE_DEGREES)
            return (360.0f / PI) * geodistFastAsinSqrt(a);
        else
            return EARTH_DIAMETER * geodistFastAsinSqrt(a);
    }
}

}

template <Method method>
class FunctionGeoDistance : public IFunction
{
public:
    static constexpr auto name =
        (method == Method::SPHERE_DEGREES) ? "greatCircleAngle"
        : ((method == Method::SPHERE_METERS) ? "greatCircleDistance"
            : "geoDistance");

private:
    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 4; }

    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        for (const auto arg_idx : collections::range(0, arguments.size()))
        {
            const auto * arg = arguments[arg_idx].get();
            if (!isNumber(WhichDataType(arg)))
                throw Exception(
                    "Illegal type " + arg->getName() + " of argument " + std::to_string(arg_idx + 1) + " of function " + getName() + ". Must be numeric",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        return std::make_shared<DataTypeFloat32>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto dst = ColumnVector<Float32>::create();
        auto & dst_data = dst->getData();
        dst_data.resize(input_rows_count);

        const IColumn & col_lon1 = *arguments[0].column;
        const IColumn & col_lat1 = *arguments[1].column;
        const IColumn & col_lon2 = *arguments[2].column;
        const IColumn & col_lat2 = *arguments[3].column;

        for (size_t row_num = 0; row_num < input_rows_count; ++row_num)
            dst_data[row_num] = distance<method>(
                col_lon1.getFloat32(row_num), col_lat1.getFloat32(row_num),
                col_lon2.getFloat32(row_num), col_lat2.getFloat32(row_num));

        return dst;
    }
};

) // DECLARE_MULTITARGET_CODE

template <Method method>
class FunctionGeoDistance : public TargetSpecific::Default::FunctionGeoDistance<method>
{
public:
    explicit FunctionGeoDistance(ContextPtr context) : selector(context)
    {
        selector.registerImplementation<TargetArch::Default,
            TargetSpecific::Default::FunctionGeoDistance<method>>();

    #if USE_MULTITARGET_CODE
        selector.registerImplementation<TargetArch::AVX,
            TargetSpecific::AVX::FunctionGeoDistance<method>>();
        selector.registerImplementation<TargetArch::AVX2,
            TargetSpecific::AVX2::FunctionGeoDistance<method>>();
        selector.registerImplementation<TargetArch::AVX512F,
            TargetSpecific::AVX512F::FunctionGeoDistance<method>>();
    #endif
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        return selector.selectAndExecute(arguments, result_type, input_rows_count);
    }

    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionGeoDistance<method>>(context);
    }

private:
    ImplementationSelector<IFunction> selector;
};

static inline Float64 degToRad(Float64 angle) { return angle * RAD_IN_DEG; }
static inline Float64 radToDeg(Float64 angle) { return angle / RAD_IN_DEG; }

/// https://en.wikipedia.org/wiki/Great-circle_distance
static Float64 greatCircleDistance(Float64 lon1Deg, Float64 lat1Deg, Float64 lon2Deg, Float64 lat2Deg)
{
    if (lon1Deg < -180 || lon1Deg > 180 ||
        lon2Deg < -180 || lon2Deg > 180 ||
        lat1Deg < -90 || lat1Deg > 90 ||
        lat2Deg < -90 || lat2Deg > 90)
    {
        throw Exception("Arguments values out of bounds for Geo function", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
    }

    Float64 lon1Rad = degToRad(lon1Deg);
    Float64 lat1Rad = degToRad(lat1Deg);
    Float64 lon2Rad = degToRad(lon2Deg);
    Float64 lat2Rad = degToRad(lat2Deg);
    Float64 u = sin((lat2Rad - lat1Rad) / 2);
    Float64 v = sin((lon2Rad - lon1Rad) / 2);
    return 2.0 * EARTH_RADIUS * asin(sqrt(u * u + cos(lat1Rad) * cos(lat2Rad) * v * v));
}

/**
 * Calculate if any element in a set of geo points is in a specific business circle, which defined by a geo position and
 * a distance(m) as radius.
 * The first three arguments of this function are constants. They defined the business circle area together.
 * Last two arguments are arrays of longitude and latitude, in which points will be tested if they are in the business circle.
 * If any of the points in the longitude and latitude array fall into the business circle defined above, the function will
 * return 1; otherwise return 0;
 * usage: inBusinessCircle(distance, longitude, latitude, longitude array, latitude array)
 */

class FunctionInBusinessCircle : public IFunction
{
public:
    static constexpr auto name = "inBusinessCircle";
    static FunctionPtr create(ContextPtr &) {return std::make_shared<FunctionInBusinessCircle>();}

private:

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 5; }
    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        for (const auto arg_idx : collections::range(0, arguments.size()))
        {
            const auto arg = arguments[arg_idx].get();
            if (arg_idx==0 && !WhichDataType(arg).isNativeUInt()) {
                throw Exception(
                        "Illegal type " + arguments[0]->getName() + " of argument 0 of function " + getName() + ". Must be UInt",
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            }
            if (arg_idx>0 && arg_idx<3 && !WhichDataType(arg).isFloat64()) {
                throw Exception(
                        "Illegal type " + arg->getName() + " of argument " + std::to_string(arg_idx + 1) + " of function " + getName() + ". Must be Float64",
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            }
            if (arg_idx>=3 && !WhichDataType(arg).isArray()) {
                throw Exception(
                        "Illegal type " + arg->getName() + " of argument " + std::to_string(arg_idx + 1) + " of function " + getName() + ". Must be Array(Float64)",
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            }
        }

        return std::make_shared<DataTypeUInt8>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & , size_t input_rows_count) const override
    {
        const auto & distance = assert_cast<const ColumnConst *>(arguments[0].column.get())->getValue<UInt64>();
        const auto & longitude = assert_cast<const ColumnConst *>(arguments[1].column.get())->getValue<Float64>();
        const auto & latitude = assert_cast<const ColumnConst *>(arguments[2].column.get())->getValue<Float64>();

        auto dst = ColumnVector<UInt8>::create();
        auto &  dst_data = dst->getData();
        dst_data.resize_fill(input_rows_count);

        Float64 locs[4];
        locs[0] = longitude;
        locs[1] = latitude;


        ColumnArray * arr_lon = assert_cast<ColumnArray *>(arguments[3].column->assumeMutable().get());
        ColumnArray * arr_lat = assert_cast<ColumnArray *>(arguments[4].column->assumeMutable().get());

        if (!arr_lon || !arr_lat) {
            throw Exception("Illegal column of last two arguments of function " + getName(), ErrorCodes::ILLEGAL_COLUMN);
        }

        const ColumnArray::Offsets & lon_offsets = arr_lon->getOffsets();
        const ColumnArray::Offsets & lat_offsets = arr_lat->getOffsets();

        IColumn * lon_data = & arr_lon->getData();
        IColumn * lat_data = & arr_lat->getData();

        IColumn * inner_lon_data;
        IColumn * inner_lat_data;

        ColumnUInt8 *lon_nullmap = nullptr;
        ColumnUInt8 *lat_nullmap = nullptr;


        if (isColumnNullable(*lon_data)) {
            ColumnNullable * c1 = static_cast<ColumnNullable *>(lon_data);
            lon_nullmap = &(c1->getNullMapColumn());
            inner_lon_data = &(c1->getNestedColumn());
        } else {
            inner_lon_data = lon_data;
        }

        if (isColumnNullable(*lat_data)) {
            ColumnNullable * c2 = static_cast<ColumnNullable *>(lat_data);
            lat_nullmap = &(c2->getNullMapColumn());
            inner_lat_data = &(c2->getNestedColumn());
        } else {
            inner_lat_data = lat_data;
        }

        const ColumnVector<Float64> * lon_data_vector =  checkAndGetColumn<ColumnVector<Float64>>(inner_lon_data);
        const ColumnVector<Float64> * lat_data_vector =  checkAndGetColumn<ColumnVector<Float64>>(inner_lat_data);

        if (!lon_data_vector || !lat_data_vector)
            throw Exception("Illegal data type in array type columns of longitude and latitude", ErrorCodes::ILLEGAL_COLUMN);


        ColumnArray::Offset prev_offset = 0;

        for (size_t row=0; row < input_rows_count; row++)
        {
            ColumnArray::Offset offset1 = lon_offsets[row];
            ColumnArray::Offset offset2 = lat_offsets[row];

            if (offset1 != offset2)
                throw Exception("Size of longitude array must equal to that of latitude array.", ErrorCodes::LOGICAL_ERROR);

            while (prev_offset<offset1)
            {
                /// skip null input
                if ((lon_nullmap && lon_nullmap->getData()[prev_offset]) || (lat_nullmap && lat_nullmap->getData()[prev_offset])) {
                    prev_offset++;
                    continue;
                }

                locs[2] = lon_data_vector->getData()[prev_offset];
                locs[3] = lat_data_vector->getData()[prev_offset];

                ///skip illegal geo position
                if (locs[2] < -180 || locs[2] > 180 ||
                    locs[3] < -90 || locs[3] > 90) {
                    prev_offset++;
                    continue;
                }

                if (greatCircleDistance(locs[0], locs[1], locs[2], locs[3]) <= distance) {
                    dst_data[row] = 1;
                    break;
                }
                prev_offset++;
            }

            prev_offset = offset1;
        }

        return dst;
    }
};

/***
 * Some optimization on inBusinessCircle function. Split into two functions for the seek of compatibility. Replace inBusinessCircle later.
 * The inBusinessCircle2 enable user provide multiple centers and may short cut calculation if any one of the provided positions is in
 * the `business circle`. Moreover, bounding box (https://www.movable-type.co.uk/scripts/latlong-db.html) is used to filter unlikely positions
 * and may reduce useless great distance calculation.
 */
class FunctionInBusinessCircle2 : public IFunction
{
public:
    static constexpr auto name = "inBusinessCircle2";
    static FunctionPtr create(ContextPtr &) {return std::make_shared<FunctionInBusinessCircle2>();}

private:

    String getName() const override { return name; }
    bool isVariadic() const override {return true; }
    size_t getNumberOfArguments() const override { return 0; }
    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() < 5 || (arguments.size()-2) % 3 != 0)
            throw Exception("Incorrect number of arguments of function " + getName() + ". Must be 2 for latitude and " +
                            "longitude array and plus 3*n for locations (distance, p_langitude, p_latitude)", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        for (const auto arg_idx : collections::range(0, arguments.size()))
        {
            const auto arg = arguments[arg_idx].get();
            if (arg_idx<2 && !WhichDataType(arg).isArray())
            {
                throw Exception(
                    "Illegal type " + arg->getName() + " of argument " + std::to_string(arg_idx + 1) + " of function " + getName() + ". Must be Array(Float64)",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            }

            if (arg_idx>=2)
            {
                if (arg_idx % 3 == 2)
                {
                    if (!WhichDataType(arg).isNativeUInt())
                        throw Exception(
                            "Illegal type " + arguments[0]->getName() + " of argument " + std::to_string(arg_idx + 1) + " of function " + getName() + ". Must be UInt",
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
                }
                else
                {
                    if (!WhichDataType(arg).isFloat64())
                        throw Exception(
                            "Illegal type " + arguments[0]->getName() + " of argument " + std::to_string(arg_idx + 1) + " of function " + getName() + ". Must be Float64",
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
                }
            }
        }

        return std::make_shared<DataTypeUInt8>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
        ColumnArray * arr_lon = assert_cast<ColumnArray *>(arguments[0].column->assumeMutable().get());
        ColumnArray * arr_lat = assert_cast<ColumnArray *>(arguments[1].column->assumeMutable().get());

        if (!arr_lon || !arr_lat) {
            throw Exception("Illegal column of last two arguments of function " + getName(), ErrorCodes::ILLEGAL_COLUMN);
        }

        const ColumnArray::Offsets & lon_offsets = arr_lon->getOffsets();
        const ColumnArray::Offsets & lat_offsets = arr_lat->getOffsets();

        IColumn * lon_data = & arr_lon->getData();
        IColumn * lat_data = & arr_lat->getData();

        IColumn * inner_lon_data;
        IColumn * inner_lat_data;

        ColumnUInt8 *lon_nullmap = nullptr;
        ColumnUInt8 *lat_nullmap = nullptr;


        if (isColumnNullable(*lon_data)) {
            ColumnNullable * c1 = static_cast<ColumnNullable *>(lon_data);
            lon_nullmap = &(c1->getNullMapColumn());
            inner_lon_data = &(c1->getNestedColumn());
        } else {
            inner_lon_data = lon_data;
        }

        if (isColumnNullable(*lat_data)) {
            ColumnNullable * c2 = static_cast<ColumnNullable *>(lat_data);
            lat_nullmap = &(c2->getNullMapColumn());
            inner_lat_data = &(c2->getNestedColumn());
        } else {
            inner_lat_data = lat_data;
        }

        const ColumnVector<Float64> * lon_data_vector =  checkAndGetColumn<ColumnVector<Float64>>(inner_lon_data);
        const ColumnVector<Float64> * lat_data_vector =  checkAndGetColumn<ColumnVector<Float64>>(inner_lat_data);

        if (!lon_data_vector || !lat_data_vector)
            throw Exception("Illegal data type in array type columns of longitude and latitude", ErrorCodes::ILLEGAL_COLUMN);

        auto dst = ColumnVector<UInt8>::create();
        auto &  dst_data = dst->getData();
        dst_data.resize_fill(input_rows_count);

        Float64 locs[4];
        Float64 max_lat, min_lat, max_lon, min_lon;

        /// outer loop user provided positions.
        size_t args_idx = 2;
        while (args_idx < arguments.size())
        {
            const auto & distance = assert_cast<const ColumnConst *>(arguments[args_idx++].column.get())->getValue<UInt64>();
            const auto & longitude = static_cast<const ColumnConst *>(arguments[args_idx++].column.get())->getValue<Float64>();
            const auto & latitude = static_cast<const ColumnConst *>(arguments[args_idx++].column.get())->getValue<Float64>();

            locs[0] = longitude;
            locs[1] = latitude;

            /// Widen the limitation by multiplying an factor 1.2
            max_lon = locs[0] + 1.2*abs(radToDeg(asin((distance > EARTH_RADIUS) ? 1 : distance/EARTH_RADIUS) / cos(degToRad(locs[1]))));
            min_lon = locs[0] - 1.2*abs(radToDeg(asin((distance > EARTH_RADIUS) ? 1 : distance/EARTH_RADIUS) / cos(degToRad(locs[1]))));
            max_lat = locs[1] + 1.2*abs(radToDeg(distance/EARTH_RADIUS));
            min_lat = locs[1] - 1.2*abs(radToDeg(distance/EARTH_RADIUS));

            ColumnArray::Offset prev_offset = 0;

            for (size_t row=0; row < input_rows_count; row++)
            {
                ColumnArray::Offset offset1 = lon_offsets[row];
                ColumnArray::Offset offset2 = lat_offsets[row];

                if (offset1 != offset2)
                    throw Exception("Size of longitude array must equal to that of latitude array.", ErrorCodes::LOGICAL_ERROR);

                /// skip if current row match any of previous position
                if (dst_data[row] == 1)
                {
                    prev_offset = offset1;
                    continue;
                }

                while (prev_offset<offset1)
                {
                    /// skip null input
                    if ((lon_nullmap && lon_nullmap->getData()[prev_offset]) || (lat_nullmap && lat_nullmap->getData()[prev_offset])) {
                        prev_offset++;
                        continue;
                    }

                    locs[2] = lon_data_vector->getData()[prev_offset];
                    locs[3] = lat_data_vector->getData()[prev_offset];

                    ///skip illegal geo position
                    if (locs[2] < -180 || locs[2] > 180 ||
                        locs[3] < -90 || locs[3] > 90) {
                        prev_offset++;
                        continue;
                    }

                    /// fast skip unlikely position
                    if (!(max_lon > locs[2] && min_lon < locs[2] && max_lat > locs[3] && min_lat < locs[3]))
                    {
                        prev_offset++;
                        continue;
                    }

                    if (greatCircleDistance(locs[0], locs[1], locs[2], locs[3]) <= distance) {
                        dst_data[row] = 1;
                        break;
                    }
                    prev_offset++;
                }

                prev_offset = offset1;
            }
        }

        return dst;
    }
};

REGISTER_FUNCTION(GeoDistance)
{
    geodistInit();
    factory.registerFunction<FunctionGeoDistance<Method::SPHERE_DEGREES>>();
    factory.registerFunction<FunctionGeoDistance<Method::SPHERE_METERS>>();
    factory.registerFunction<FunctionGeoDistance<Method::WGS84_METERS>>();
    factory.registerFunction<FunctionInBusinessCircle>();
    factory.registerFunction<FunctionInBusinessCircle2>();
}

}

