# frozen_string_literal: true

require 'mkmf'

$CFLAGS << ' -Wno-undef'
$CFLAGS << ' -Wno-discarded-qualifiers'
$CFLAGS << ' -Wno-unused-function'

# Development only: build with AddressSanitizer (EXTRALITE_ASAN=1), then run
# the tests with `bundle exec rake test:asan` (see Rakefile).
#
# -fgnu89-inline: several non-static functions in the C files are declared
# `inline` without an extern definition, which only links when every call is
# inlined (as with the default -O3).
if ENV['EXTRALITE_ASAN']
  $CFLAGS << ' -fsanitize=address -fno-omit-frame-pointer -O1 -g -fgnu89-inline'
  $LDFLAGS << ' -fsanitize=address'
  # No stack instrumentation: Ruby raises exceptions with longjmp, which
  # leaves poisoned stack redzones behind and causes false reports later.
  # It also keeps local VALUEs on the real stack, where the GC can see them.
  $CFLAGS << (try_cflags('--param=asan-stack=0') ? ' --param=asan-stack=0' : ' -mllvm -asan-stack=0')
  # SQLite serves small allocations (including statements) from its own
  # lookaside pool, which hides use-after-free bugs from ASan.
  $defs << '-DSQLITE_DEFAULT_LOOKASIDE=0,0'
end

# enable the session extension
$defs << '-DSQLITE_ENABLE_SESSION'
$defs << '-DSQLITE_ENABLE_PREUPDATE_HOOK'
$defs << '-DEXTRALITE_ENABLE_CHANGESET'

$defs << '-DHAVE_SQLITE3_ENABLE_LOAD_EXTENSION'
$defs << '-DHAVE_SQLITE3_LOAD_EXTENSION'
$defs << '-DHAVE_SQLITE3_PREPARE_V2'
$defs << '-DHAVE_SQLITE3_ERROR_OFFSET'
$defs << '-DHAVE_SQLITE3SESSION_CHANGESET'

have_func('usleep')
dir_config('extralite_ext')
create_makefile('extralite_ext')
