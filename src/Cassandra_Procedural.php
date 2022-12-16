<?php

/**
 * Procedure style wrapper for Cassandra class.
 */

require_once('Cassandra.php');

/**
 * Connects to a Cassandra node.
 *
 * @param string $host   Host name/IP to connect to.
 * @param string $user   Username in case authentication is needed.
 * @param string $passwd Password in case authentication is needed.
 * @param string $dbname Keyspace to use upon connection.
 * @param int    $port   Port to connect to.
 *
 * @return obj The created object. FALSE if unable to connect.
 */
function cassandra_connect($host, $user = '', $passwd = '', $dbname = '', $port = 9042)
{
    $obj = new Cassandra();
    if ($obj->connect($host, $user, $passwd, $dbname, $port))
        return $obj;
}

/**
 * Closes an opened connection.
 *
 * @param object $obj The object returned by cassandra_connect().
 *
 * @return int 1
 */
function cassandra_close($obj)
{
    return $obj->close();
}

/**
 * Queries the database using the given CQL.
 *
 * @param object $obj         The object returned by cassandra_connect().
 * @param string $cql         The query to run.
 * @param int    $consistency Consistency level for the operation.
 *
 * @return array Result of the query. Might be an array of rows (for SELECT),
 *               or the operation's result (for USE, CREATE, ALTER, UPDATE).
 *               NULL on error.
 */
function cassandra_query($obj, $cql, $consistency = CASSANDRA_CONSISTENCY_ALL)
{
    return $obj->query($cql, $consistency);
}

/**
 * Prepares a query statement.
 *
 * @param object $obj The object returned by cassandra_connect().
 * @param string $cql The query to prepare.
 *
 * @return array The statement's information to be used with the execute
 *               method. NULL on error.
 */
function cassandra_prepare($obj, $cql)
{
    return $obj->prepare($cql);
}

/**
 * Executes a prepared statement.
 *
 * @param object $obj         The object returned by cassandra_connect().
 * @param array  $stmt        The prepared statement as returned from the
 *                            prepare method.
 * @param array  $values      Values to bind in key=>value format where key is
 *                            the column's name.
 * @param int    $consistency Consistency level for the operation.
 *
 * @return array Result of the execution. Might be an array of rows (for
 *               SELECT), or the operation's result (for USE, CREATE, ALTER,
 *               UPDATE).
 *               NULL on error.
 */
function cassandra_execute($obj, $stmt, $values, $consistency = CASSANDRA_CONSISTENCY_ALL)
{
    return $obj->execute($stmt, $values, $consistency);
}
