#pragma once

#include <Storages/IndexFile/Options.h>

namespace DB::IndexFile
{
class Iterator;

// The returned iterator takes ownership of child.
Iterator * NewSelectIterator(Iterator * child, Predicate select_predicate);

}
