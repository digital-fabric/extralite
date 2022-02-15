# frozen_string_literal: true

require 'mkmf'

$CFLAGS << " -Wno-undef"
$CFLAGS << " -Wno-discarded-qualifiers"

dir_config('extralite_ext')
create_makefile('extralite_ext')
