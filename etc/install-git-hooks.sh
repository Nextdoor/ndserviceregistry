#!/bin/bash
#
# Creates symlinks under .git/hooks to hook scripts in the githooks
# directory
#
# Author: jacob@nextdoor.com (Jacob Hesch)

hooks="
    commit-msg
    prepare-commit-msg
"

git_hook_dir=$(dirname $0)/../.git/hooks

# The githooks directory relative to .git/hooks.
hook_dir_relative=../../etc/githooks

for hook in $hooks; do
    ln -sf $hook_dir_relative/$hook $git_hook_dir/$hook
done

# Configure git change to use our review host
git config git-change.gerrit-ssh-host review.opensource.nextdoor.com
