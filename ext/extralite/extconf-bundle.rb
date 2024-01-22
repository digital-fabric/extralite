# frozen_string_literal: true

require 'mkmf'

$CFLAGS << ' -Wno-undef'
$CFLAGS << ' -Wno-discarded-qualifiers'
$CFLAGS << ' -Wno-unused-function'

# enable the session extension
$defs << '-DSQLITE_ENABLE_SESSION'
$defs << '-DSQLITE_ENABLE_PREUPDATE_HOOK'

$defs << '-DHAVE_SQLITE3_ENABLE_LOAD_EXTENSION'
$defs << '-DHAVE_SQLITE3_LOAD_EXTENSION'
$defs << '-DHAVE_SQLITE3_PREPARE_V2'
$defs << '-DHAVE_SQLITE3_ERROR_OFFSET'
$defs << '-DHAVE_SQLITE3CHANGESET_NEW'

have_func('usleep')

dir_config('extralite_ext')
create_makefile('extralite_ext')
