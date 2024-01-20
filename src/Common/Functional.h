#include <tuple>

template<typename T, typename ...Args>
auto tuple_append(T&& t, Args&&...args)
{
    return std::tuple_cat(
        std::forward<T>(t),
        std::forward_as_tuple(args...)
    );
}

template<typename F, typename ...FrontArgs>
decltype(auto) bind_front(F&& f, FrontArgs&&...frontArgs)
{
    return [f=std::forward<F>(f),
            frontArgs = std::make_tuple(std::forward<FrontArgs>(frontArgs)...)]
            (auto&&...backArgs)
        {
            return std::apply(
                f,
                tuple_append(
                    frontArgs,
                    std::forward<decltype(backArgs)>(backArgs)...));
        };
}
