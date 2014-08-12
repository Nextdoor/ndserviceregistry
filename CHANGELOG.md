## Version 1.0.0-alpha
 * #49: Do not set ACLs on recursively created paths.
 * #39: Completely remove get_data() calls from the children of watched paths.
 * #48: Fix compatibility with Kazoo 2.0

## Version 0.3.2
 * Bugfix: Ensure Kazoo is between 1.3 and 2.0.

## Version 0.3.1
 * Bugfix: Remove *nose* requirement from the setup.py.

## Version 0.3.0
 * #45: Prevent duplicat callback executions when data has not changed
 * #39: Reduce the number of Zookeeper get() calls made by the Watcher objects
 * #38: Ensure that an non-existent path will get a Child Watch when available
 * #37: Fix DummyWatcher behavior when no local cache data is available
 * #36: Add a get_state() method to KazooServiceRegistry
 * #35: Prevent caching of nodes where *data* and *stat* are None
 * #28: Provide normal *directory style* format for ndsr output
 * #27: Disable Rate Limiting of Zookeeper calls in the CLI *ndsr* tool
 * #25: Add DataNode object type and appropriate set_data() methods
 * #15: Implemented initial Unit and Integration tests
 * #14/#24: Add in a CLI Management Command *ndsr* for interacting with Zookeeper data
 * Testing: Integrated Travis CI testing
 * Add contrib.django.utils package for easy Django/Zookeeper integration

## Version 0.2.9
 * Fix Registration.registration again - set the *ephemeral* setting at construction time.

## Version 0.2.8
 * Fix Registration.registration - set _ephemeral = False in __init__.

## Version 0.2.7
 * #20: Fix Registration.update() method - check that type(state) is bool.

## Version 0.2.6
 * Quiet down the Kazoo logger

## Version 0.2.5
 * #18: Fix bogus log.debug() statement that caused KeyError exception.
 * #19: Make sure we call set_state any time *state* is a boolean.

## Version 0.2.4
 * Bugfix: If kazoo.Semaophore() is in canceled state, retry the lock.
 * Bugfix: release_lock() was calling _get_lock() with missing parameters
 * Bugfix: Cancel a pending lock when Lock.release() is called
 * #17: Replace time.sleep() with client.handler.sleep_func()
 * #16: Add *wait=X* option to Lock objects

## Version 0.2.3
 *  Misc documentation cleanup
 *  Added Gerrit code review docs and hooks
 *  Explicitly filter out Kazoo *PING* messages with a Logging Filter
 *  Moved *Fork Detected* messages to logging.INFO
 *  #13: Use package-level logging objects rather than class-level
 *  #12: Support for Kazoo 1.0b1 enhancement
 *  #11: Implement a Lock handler model enhancement
 *  #10: Zookeeper API throttling should be configurable
 *  #9: Exception NoNodeError can be thrown when re-connecting to Zookeeper after connection loss

## Version 0.2.2
 *  Quiet down *shutdown* function
 *  Clean up the logging object names
 *  #8: Fix funcs.save_dict() when permissions are incorrect
 *  #6: Fix *on fork* behavior
