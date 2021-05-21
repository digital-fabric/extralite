# frozen_string_literal: true

require 'rubygems'
require 'mkmf'

#   $CFLAGS << " -Wno-comment"
#   $CFLAGS << " -Wno-unused-result"
#   $CFLAGS << " -Wno-dangling-else"
#   $CFLAGS << " -Wno-parentheses"
# end

dir_config 'extralite_ext'
create_makefile 'extralite_ext'
