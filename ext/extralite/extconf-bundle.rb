# frozen_string_literal: true

require 'mkmf'

$CFLAGS << ' -Wno-undef'
$CFLAGS << ' -Wno-discarded-qualifiers'
$CFLAGS << ' -Wno-unused-function'

$defs << "-DHAVE_SQLITE3_ENABLE_LOAD_EXTENSION"
$defs << "-DHAVE_SQLITE3_LOAD_EXTENSION"
$defs << "-DHAVE_SQLITE3_ERROR_OFFSET"

have_func('usleep')

dir_config('extralite_ext')
create_makefile('extralite_ext')
