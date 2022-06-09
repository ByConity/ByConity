#pragma once

namespace DB
{

enum class ManipulationType : unsigned int
{
    Empty = 0,
    Insert,
    Merge,
    Drop,
    Fetch,
    Mutate,
    BuildBitmap,
    Dump,
    Clustering,
};


inline constexpr const char * typeToString(ManipulationType type)
{
    switch (type)
    {
        case ManipulationType::Empty:
            return "Empty";
        case ManipulationType::Insert:
            return "Insert";
        case ManipulationType::Merge:
            return "Merge";
        case ManipulationType::Mutate:
            return "Mutate";
        case ManipulationType::Clustering:
            return "Clustering";
        default:
            /// TODO:
            return "Unknown";
    }
}

}
