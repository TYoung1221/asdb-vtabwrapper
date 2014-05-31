ASDB - Any Source Database
====

Install
----
default
```
$ mkdir build
$ cd build
$ cmake ..
$ make
$ sudo make install
```

Ubuntu
```
$ mkdir build
$ cd build
$ cmake ..
$ cpack
$ sudo dpkg -i libasdb-dev-0.3.0-Linux.dev
```

Example
----
```
$ cd example
$ gcc -std=gnu99 -Wl,-rpath=. -shared -fPIC -o engtext.so engtext.c -lasdb -lsqlite3
$ sqlite3
> .load "./engtext"
> create virtual table t using text (./engtext.c);
> select ROWID,* from t;
```

License
---
MIT License
