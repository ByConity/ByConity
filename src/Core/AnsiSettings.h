#pragma once

namespace DB {

struct Settings;

namespace ANSI {

void onSettingChanged(Settings *s);

}
}
