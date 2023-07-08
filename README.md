# uri2x/php-cql
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Last update : 2023/07/08

Native [Apache Cassandra](https://cassandra.apache.org) and [ScyllaDB](https://www.scylladb.com) connector for PHP based on the CQL binary protocol (v3),
without the need for an external extension.

Requires [PHP](https://www.php.net/) version >5, Cassandra >1.2, and any ScyllaDB version.

Installation
------------

Either:

* Via [Composer](https://getcomposer.org/):
```bash
$ composer require uri2x/php-cql
```
OR

* Copy `Cassandra.php` to your project and include it.

Important
---------

Make sure you turn on the native transport for Cassandra by editing your
cassandra.yaml file and adding the following line:

    start_native_transport: true

Usage
-----

#### Available methods

connect($host, $user = '', $passwd = '', $dbname = '', $port = 9042)

    Connects to a Cassandra node.

    @param string $host   Host name/IP to connect to use 'p:' as prefix for persistent connections.
    @param string $user   Username in case authentication is needed.
    @param string $passwd Password in case authentication is needed.
    @param string $dbname Keyspace to use upon connection.
    @param int    $port   Port to connect to.
    @param int    $retries: Number of connection retries (default: 3, useful for persistent connections in case of timeouts).

    @return int The socket descriptor used. FALSE if unable to connect.

close()

    Closes an opened connection.

    @return int 1

query($cql, $consistency = CASSANDRA_CONSISTENCY_ALL, $values = [])

    Queries the database using the given CQL.

    @param string $cql         The query to run.
    @param int    $consistency Consistency level for the operation.
    @param array  $values      Values to bind in a sequential or key=>value format,
                               where key is the column's name.

    @return array Result of the query. Might be an array of rows (for SELECT),
                  or the operation's result (for USE, CREATE, ALTER, UPDATE).
                  NULL on error.

bind_param($value, $column_type)
     Returns a binded parameter to be used with the query method (static method)

     @param mixed $value Value to bind        The query to run.
     @param int   $type  Value type out of one of the Cassandra::COLUMNTYPE_* constants

     @return array value to be used as part of the $values parameter of the query method

prepare($cql)

    Prepares a query.

    @param string $cql The query to prepare.

    @return array The statement's information to be used with the execute
                  method. NULL on error.


execute($stmt, $values, $consistency = CASSANDRA_CONSISTENCY_ALL)

    Executes a prepared statement.

    @param array $stmt        The prepared statement as returned from the
                              prepare method.
    @param array $values      Values to bind in key=>value format where key is
                              the column's name.
    @param int   $consistency Consistency level for the operation.

    @return array Result of the execution. Might be an array of rows (for
                  SELECT), or the operation's result (for USE, CREATE, ALTER,
                  UPDATE).
                  NULL on error.
#### Procedural
In addition, a wrapper has been made for those who prefer to work with
procedural programming. To use the wrapper, make sure to include
`Cassandra_Procedural.php` that contains the following methods:

cassandra_connect($host, $user = '', $passwd = '', $dbname = '', $port = 9042)

    Same as $Cassandra->connect() above. Returns an object type if connection
    was successfull. Otherwise returns NULL.

cassandra_close($obj)

    Same as $Cassandra->close() above. Use $obj from cassandra_connect as the
    first parameter.

cassandra_query($obj, $cql, $consistency = CASSANDRA_CONSISTENCY_ALL, $values = [])

    Same as $Cassandra->query() above. Use $obj from cassandra_connect as the
    first parameter.

 cassandra_bind_param($value, $column_type)

    Same as $Cassandra->bind_param() above

cassandra_prepare($obj, $cql)

    Same as $Cassandra->prepare() above. Use $obj from cassandra_connect as the
    first parameter.

cassandra_execute($obj, $stmt, $values, $consistency = CASSANDRA_CONSISTENCY_ALL)

    Same as $Cassandra->execute() above. Use $obj from cassandra_connect as the
    first parameter.


Sample usage
------------
```php

<?php

require_once('vendor/autoload.php');

use CassandraNative\Cassandra;

$obj = new Cassandra();

// Connects to the node:
$res = $obj->connect('127.0.0.1', 'my_user', 'my_pass', 'my_keyspace');

// Tests if the connection was successful:
if ($res)
{
    // Queries a table:
    $arr = $obj->query('SELECT col1, col2, col3 FROM my_table WHERE id=?',
      Cassandra::CONSISTENCY_ONE,
      [Cassandra::bind_param(1001, Cassandra::COLUMNTYPE_BIGINT]);

    // $arr, for example, may contain:
    // Array
    // (
    //     [0] => Array
    //         (
    //             [col1] => first row
    //             [col2] => 1
    //             [col3] => 0x111111
    //         )
    //
    //     [1] => Array
    //         (
    //             [col1] => second row
    //             [col2] => 2
    //             [col3] => 0x222222
    //         )
    //
    // )

    // Prepares a statement:
    $stmt = $obj->prepare('UPDATE my_table SET col2=?,col3=? WHERE col1=?');

    // Executes a prepared statement:
    $values = ['col2' => 5, 'col3' => '0x55', 'col1' => 'five'];
    $pResult = $obj->execute($stmt, $values);

    // Upon success, $pResult would be:
    // Array
    // (
    //     [0] => Array
    //         (
    //             [result] => success
    //         )
    //
    // )

    // Closes the connection:
    $obj->close();
}
```
or, same as above in procedural style:
```php
// Connects to the node:
$handle = cassandra_connect('127.0.0.1', 'my_user', 'my_pass', 'my_keyspace');

// Tests if the connection was successful:
if ($handle)
{
    // Queries a table:
    $arr = cassandra_query($handle, 'SELECT col1, col2, col3 FROM my_table');

    // Prepares a statement:
    $stmt = cassandra_prepare($handle,
        'UPDATE my_table SET col2=?,col3=? WHERE col1=?');

    // Executes a prepared statement:
    $values = ['col2' => 5, 'col3' => '0x55', 'col1' => 'five'];
    $pResult = cassandra_execute($handle, $stmt, $values);

    // Closes the connection:
    cassandra_close($handle);
}
```

External links
--------------

1. Datastax's blog introducing the binary protocol:
http://www.datastax.com/dev/blog/binary-protocol

2. CQL definitions
https://cassandra.apache.org/_/native_protocol.html


License
-------

    The MIT License (MIT)

    Copyright (c) 2023 Uri Hartmann

    Permission is hereby granted, free of charge, to any person obtaining a
    copy of this software and associated documentation files (the "Software"),
    to deal in the Software without restriction, including without limitation
    the rights to use, copy, modify, merge, publish, distribute, sublicense,
    and/or sell copies of the Software, and to permit persons to whom the
    Software is furnished to do so, subject to the following conditions:

    The above copyright notice and this permission notice shall be included in
    all copies or substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
    IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
    LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
    FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
    DEALINGS IN THE SOFTWARE.
