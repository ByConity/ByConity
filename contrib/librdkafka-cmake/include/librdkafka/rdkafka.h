#if __has_include(<rdkafka.h>) // maybe bundled
#    include_next <rdkafka.h>
#else // system
#    include_next <../../librdkafka/src/rdkafka.h>
#endif
