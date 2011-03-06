=========
ChangeLog
=========

Users should ignore this content: **it is draft**.

Contributors should add entries here in the following section, on top of the
others.

`WIP (coming releases)`
=======================

New Features
------------

* optional: experimental SQLite-based backend for the LocalStatus
  cache. Plain text remains the default. Enable by setting
  status_backend=sqlite in the local [Repository ...] section

Changes
-------

* Increase compatability with Gmail servers which claim to not support
  the UIDPLUS extension but in reality do.

Bug Fixes
---------

* Fix hang when using Ctrl+C in some cases.


Pending for the next major release
==================================

* UIs get shorter and nicer names. (API changing)
* Implement IDLE feature. (delayed until next major release)
