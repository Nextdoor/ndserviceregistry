Version 0.2.6
============================================================
*   Quiet down the Kazoo logger

Version 0.2.5
============================================================
*   #18: Fix bogus log.debug() statement that caused KeyError exception.
*   #19: Make sure we call set_state any time 'state' is a boolean.

Version 0.2.4
============================================================
*   Bugfix: If kazoo.Semaophore() is in canceled state, retry the lock.
*   Bugfix: release_lock() was calling _get_lock() with missing parameters
*   Bugfix: Cancel a pending lock when Lock.release() is called
*   #17: Replace time.sleep() with client.handler.sleep_func()
*   #16: Add 'wait=X' option to Lock objects

Version 0.2.3
============================================================

*    Misc documentation cleanup
*    Added Gerrit code review docs and hooks
*    Explicitly filter out Kazoo 'PING' messages with a Logging Filter
*    Moved 'Fork Detected' messages to logging.INFO
*    #13: Use package-level logging objects rather than class-level
*    #12: Support for Kazoo 1.0b1 enhancement
*    #11: Implement a Lock handler model enhancement
*    #10: Zookeeper API throttling should be configurable
*    #9: Exception NoNodeError can be thrown when re-connecting to Zookeeper after connection loss


Version 0.2.2
============================================================

*    Quiet down 'shutdown' function
*    Clean up the logging object names
*    #8: Fix funcs.save_dict() when permissions are incorrect
*    #6: Fix 'on fork' behavior

