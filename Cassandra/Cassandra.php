<?php

/**
 * Cassanda Connector
 *
 * A native Cassandra connector for PHP based on the CQL binary protocol v1,
 * without the need for any external extensions.
 *
 * Requires PHP version 5, and Cassandra >1.2.
 *
 * Usage and more information is found on docs/Cassandra.txt
 *
 * The MIT License (MIT)
 *
 * Copyright (c) 2014 Uri Hartmann
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 *
 * @category  Database
 * @package   Cassandra
 * @author    Uri Hartmann
 * @copyright 2014 Uri Hartmann
 * @license   http://opensource.org/licenses/MIT The MIT License (MIT)
 * @version   2014.04.29
 * @link      http://www.humancodes.org/projects/php-cassandra
 */

class Cassandra
{
    const CONSISTENCY_ANY          = 0x0000;
    const CONSISTENCY_ONE          = 0x0001;
    const CONSISTENCY_TWO          = 0x0002;
    const CONSISTENCY_THREE        = 0x0003;
    const CONSISTENCY_QUORUM       = 0x0004;
    const CONSISTENCY_ALL          = 0x0005;
    const CONSISTENCY_LOCAL_QUORUM = 0x0006;
    const CONSISTENCY_EACH_QUORUM  = 0x0007;
    const CONSISTENCY_LOCAL_ONE    = 0x000A;

    const COLUMNTYPE_CUSTOM    = 0x0000;
    const COLUMNTYPE_ASCII     = 0x0001;
    const COLUMNTYPE_BIGINT    = 0x0002;
    const COLUMNTYPE_BLOB      = 0x0003;
    const COLUMNTYPE_BOOLEAN   = 0x0004;
    const COLUMNTYPE_COUNTER   = 0x0005;
    const COLUMNTYPE_DECIMAL   = 0x0006;
    const COLUMNTYPE_DOUBLE    = 0x0007;
    const COLUMNTYPE_FLOAT     = 0x0008;
    const COLUMNTYPE_INT       = 0x0009;
    const COLUMNTYPE_TEXT      = 0x000A;
    const COLUMNTYPE_TIMESTAMP = 0x000B;
    const COLUMNTYPE_UUID      = 0x000C;
    const COLUMNTYPE_VARCHAR   = 0x000D;
    const COLUMNTYPE_VARINT    = 0x000E;
    const COLUMNTYPE_TIMEUUID  = 0x000F;
    const COLUMNTYPE_INET      = 0x0010;
    const COLUMNTYPE_LIST      = 0x0020;
    const COLUMNTYPE_MAP       = 0x0021;
    const COLUMNTYPE_SET       = 0x0022;

    const OPCODE_ERROR          = 0x00;
    const OPCODE_STARTUP        = 0x01;
    const OPCODE_READY          = 0x02;
    const OPCODE_AUTHENTICATE   = 0x03;
    const OPCODE_CREDENTIALS    = 0x04;
    const OPCODE_OPTIONS        = 0x05;
    const OPCODE_SUPPORTED      = 0x06;
    const OPCODE_QUERY          = 0x07;
    const OPCODE_RESULT         = 0x08;
    const OPCODE_PREPARE        = 0x09;
    const OPCODE_EXECUTE        = 0x0A;
    const OPCODE_REGISTER       = 0x0B;
    const OPCODE_EVENT          = 0x0C;
    const OPCODE_BATCH          = 0x0D;
    const OPCODE_AUTH_CHALLENGE = 0x0E;
    const OPCODE_AUTH_RESPONSE  = 0x0F;
    const OPCODE_AUTH_SUCCESS   = 0x10;

    const RESULT_KIND_VOID          = 0x001;
    const RESULT_KIND_ROWS          = 0x002;
    const RESULT_KIND_SET_KEYSPACE  = 0x003;
    const RESULT_KIND_PREPARED      = 0x004;
    const RESULT_KIND_SCHEMA_CHANGE = 0x005;

    private $socket = 0;

    public function __construct()
    {
    }

    /* Makes sure to close() upon destruct */
    function __destruct()
    {
        if ($this->socket)
            $this->close();
    }

    /**
     * Connects to a Cassandra node.
     *
     * @param string $host   Host name/IP to connect to.
     * @param string $user   Username in case authentication is needed.
     * @param string $passwd Password in case authentication is needed.
     * @param string $dbname Keyspace to use upon connection.
     * @param int    $port   Port to connect to.
     *
     * @return int The socket descriptor used. FALSE if unable to connect.
     * @access public
     */
    public function connect($host, $user = '', $passwd = '', $dbname = '', $port = 9042)
    {
        // Lookups host name to IP, if needed
        if ($this->socket)
            $this->close();

        $this->socket = 0;

        $hostIsIP = filter_var($host, FILTER_VALIDATE_IP,
            FILTER_FLAG_IPV4 | FILTER_FLAG_IPV6);

        if (!$hostIsIP)
        {
            if (($ip = @gethostbyname($host)) === false)
            {
                trigger_error('Host lookup failed: '.
                    socket_strerror(socket_last_error()));
                return FALSE;
            }
        } else
        {
            $ip = $host;
        }

        $isIPV6 = filter_var($ip, FILTER_VALIDATE_IP, FILTER_FLAG_IPV6);
        $domain = ($isIPV6 ? AF_INET6 : AF_INET);

        // Creates socket
        $socket = @socket_create($domain, SOCK_STREAM, SOL_TCP);
        if ($socket === false)
        {
            trigger_error('Socket creation failed: '.
                socket_strerror(socket_last_error()));
            socket_close($socket);
            return FALSE;
        }

        // Connects to server
        if (@socket_connect($socket, $ip, $port) === false)
        {
            trigger_error('Socket connect failed: '.
                socket_strerror(socket_last_error()));
            socket_close($socket);
            return FALSE;
        }

        $this->socket = $socket;

        // Writes a STARTUP frame
        $frameBody = self::pack_string_map(array('CQL_VERSION' => '3.0.0'));
        if (!$this->write_frame(self::OPCODE_STARTUP, $frameBody))
        {
            $this->close();
            return FALSE;
        }

        // Reads incoming frame
        if (($frame = $this->read_frame()) === 0)
        {
            $this->close();
            return FALSE;
        }

        // Checks if an AUTHENTICATE frame was received
        if ($frame['opcode'] == self::OPCODE_AUTHENTICATE)
        {
            // Writes a CREDENTIALS frame
            $body =
                self::pack_short(2).
                self::pack_string('username').
                self::pack_string($user).
                self::pack_string('password').
                self::pack_string($passwd);
            if (!$this->write_frame(self::OPCODE_CREDENTIALS, $body))
            {
                $this->close();
                return FALSE;
            }

            // Reads incoming frame
            if (($frame = $this->read_frame()) === 0)
            {
                $this->close();
                return FALSE;
            }
        }

        // Checks if a READY frame was received
        if ($frame['opcode'] != self::OPCODE_READY)
        {
            $this->close();
            trigger_error('Missing READY packet. Got '.
                $frame['opcode'].') instead');
            return FALSE;
        }

        // Checks if we need to set initial keyspace
        if ($dbname)
        {
            // Sends a USE query.
            $res = $this->query('USE '.$dbname);

            // Checks the validity of the response
            if (!isset($res[0]) || ($res[0]['keyspace'] != $dbname))
            {
                $this->close();
                return FALSE;
            }
        }

        // Returns the socket on success
        return $this->socket;
    }

    /**
     * Closes an opened connection.
     *
     * @return int 1
     * @access public
     */
    public function close()
    {
        if ($this->socket)
        {
            socket_close($this->socket);
            $this->socket = 0;
        }
        return 1;
    }

    /**
     * Queries the database using the given CQL.
     *
     * @param string $cql         The query to run.
     * @param int    $consistency Consistency level for the operation.
     *
     * @return array Result of the query. Might be an array of rows (for
     *               SELECT), or the operation's result (for USE, CREATE,
     *               ALTER, UPDATE).
     *               NULL on error.
     * @access public
     */
    public function query($cql, $consistency = self::CONSISTENCY_ALL)
    {
        // Prepares the frame's body
        $frame = self::pack_long_string($cql).
            self::pack_short($consistency);

        // Writes a QUERY frame and return the result
        return $this->request_result(self::OPCODE_QUERY, $frame);
    }

    /**
     * Prepares a query statement.
     *
     * @param string $cql The query to prepare.
     *
     * @return array The statement's information to be used with the execute
     *               method. NULL on error.
     * @access public
     */
    public function prepare($cql)
    {
        // Prepares the frame's body
        $frame = self::pack_long_string($cql);

        // Writes a PREPARE frame and return the result
        return $this->request_result(self::OPCODE_PREPARE, $frame);
    }

    /**
     * Executes a prepared statement.
     *
     * @param array $stmt        The prepared statement as returned from the
     *                           prepare method.
     * @param array $values      Values to bind in key=>value format where key is
     *                           the column's name.
     * @param int   $consistency Consistency level for the operation.
     *
     * @return array Result of the execution. Might be an array of rows (for
     *               SELECT), or the operation's result (for USE, CREATE,
     *               ALTER, UPDATE).
     *               NULL on error.
     * @access public
     */
    public function execute($stmt, $values, $consistency = self::CONSISTENCY_ALL)
    {
        // Prepares the frame's body - <id><count><values map>
        $frame = base64_decode($stmt['id']);
        $frame = self::pack_string($frame).self::pack_short(count($values));

        foreach ($stmt['columns'] as $key => $column)
        {
            $value = $values[$key];

            $data = self::pack_value($value, $column['type'],
                $column['subtype1'], $column['subtype2']);

            $frame .= self::pack_long_string($data);
        }
        $frame .= self::pack_short($consistency);

        // Writes a EXECUTE frame and return the result
        return $this->request_result(self::OPCODE_EXECUTE, $frame);
    }

    /**
     * Writes a (QUERY/PREPARE/EXCUTE) frame, reads the result, and parses it.
     *
     * @param int    $opcode Frame's opcode.
     * @param string $body   Frame's body.
     *
     * @return array Result of the request. Might be an array of rows (for
     *               SELECT), or the operation's result (for USE, CREATE,
     *               ALTER, UPDATE).
     *               NULL on error.
     * @access private
     */
    private function request_result($opcode, $body)
    {
        // Writes the frame
        if (!$this->write_frame($opcode, $body))
            return NULL;

        // Reads incoming frame
        $frame = $this->read_frame();
        if (!$frame)
            return NULL;

        // Parses the incoming frame
        if ($frame['opcode'] == self::OPCODE_RESULT)
        {
            return $this->parse_result($frame['body']);
        } else
        {
            trigger_error('Unknown opcode '.$frame['opcode']);
            return NULL;
        }
    }

    /**
     * Packs and writes a frame to the socket.
     *
     * @param int    $opcode   Frame's opcode.
     * @param string $body     Frame's body.
     * @param int    $response Frame's response flag.
     *
     * @return bool true on success, false on error.
     * @access private
     */
    private function write_frame($opcode, $body, $response = 0)
    {
        // Prepares the outgoing packet
        $frame = self::pack_frame($opcode, $body, $response);

        // Writes frame to socket
        if (@socket_write($this->socket, $frame) === false)
        {
            trigger_error('Socket write failed: '.
                socket_strerror(socket_last_error()));
            return false;
        }

        return true;
    }

    /**
     * Reads data with a specific size from socket.
     *
     * @param int $size Requested data size.
     *
     * @return string Incoming data, false on error.
     * @access private
     */
    private function read_size($size)
    {
        if (!$this->socket)
            return false;

        $data = '';
        while (strlen($data) < $size)
        {
            $buff = socket_read($this->socket, $size, PHP_BINARY_READ);
            if ($buff === false)
                return false;
            $data .= $buff;
        }
        return $data;
    }

    /**
     * Reads pending frame from the socket.
     *
     * @return string Incoming data, false on error.
     * @access private
     */
    private function read_frame()
    {
        // Read the 8 bytes header
        if (!($header = $this->read_size(8)))
        {
            trigger_error('Missing header ('.strlen($header).')');
            return false;
        }

        // Unpack the header to its contents:
        // <byte version><byte flags><byte stream><byte opcode><int length>
        $opcode = ord($header[3]);
        $length = self::int_from_bin($header, 4, 4, 0);

        // Read frame body, if exists
        if ($length)
        {
            if (!($body = $this->read_size($length)))
            {
                return false;
            }
        } else
        {
            $body = '';
        }

        // If we got an error - trigger it and return an error
        if ($opcode == self::OPCODE_ERROR)
        {
            // ERROR: <int code><string msg>
            $errCode = self::int_from_bin($body, 0, 4);
            $bodyOffset = 4;  // Must be passed by reference
            $errMsg = self::pop_string($body, $bodyOffset);

            trigger_error('Error 0x'.sprintf('%04X', $errCode).
                ' received from server: '.$errMsg);

            return false;
        }

        return array('opcode' => $opcode, 'body' => $body);
    }

    /**
     * Parses a RESULT frame.
     *
     * @param string $body Frame's body
     *
     * @return array Parsed frame. Might be an array of rows (for SELECT),
     *               or the operation's result (for USE, CREATE, ALTER,
     *               UPDATE).
     *               NULL on error.
     * @access private
     */
    private static function parse_result($body)
    {
        // Parse RESULTS opcode
        $bodyOffset = 0;
        $kind = self::pop_int($body, $bodyOffset);

        switch ($kind)
        {
            case self::RESULT_KIND_VOID:
                return array(array('result' => 'success'));
            case self::RESULT_KIND_ROWS:
                return self::parse_rows($body, $bodyOffset);
            case self::RESULT_KIND_SET_KEYSPACE:
                $keyspace = self::pop_string($body, $bodyOffset);
                return array(array('keyspace' => $keyspace));
            case self::RESULT_KIND_PREPARED:
                // <string id><metadata>
                $id = base64_encode(self::pop_string($body, $bodyOffset));
                $metadata = self::parse_rows_metadata($body, $bodyOffset);
                $columns = array();

                foreach ($metadata as $column)
                {
                    $columns[$column['name']] = array(
                        'type' => $column['type'],
                        'subtype1' => $column['subtype1'],
                        'subtype2' => $column['subtype2']
                    );
                }

                return array('id' => $id, 'columns' => $columns);
            case self::RESULT_KIND_SCHEMA_CHANGE:
                // <string change><string keyspace><string table>
                $change = self::pop_string($body, $bodyOffset);
                $keyspace = self::pop_string($body, $bodyOffset);
                $table = self::pop_string($body, $bodyOffset);
                return array(array('change' => $change, 'keyspace' => $keyspace,
                    'table' => $table));
        }
        trigger_error('Unknown result kind '.$kind);
    }

    /**
     * Parses a RESULT Rows metadata (also used for RESULT Prepared), starting
     * from the offset, and advancing it in the process.
     *
     * @param string $body       Metadata body.
     * @param string $bodyOffset Metadata body offset to start from.
     *
     * @return array Columns list
     * @access private
     */
    private static function parse_rows_metadata($body, &$bodyOffset)
    {
        $flags = self::pop_int($body, $bodyOffset);
        $columns_count = self::pop_int($body, $bodyOffset);

        $global_table_spec = ($flags & 0x0001);
        if ($global_table_spec)
        {
            $keyspace = self::pop_string($body, $bodyOffset);
            $table = self::pop_string($body, $bodyOffset);
        }

        $columns = array();

        for ($i=0;$i<$columns_count;$i++)
        {
            if (!$global_table_spec)
            {
                $keyspace = self::pop_string($body, $bodyOffset);
                $table = self::pop_string($body, $bodyOffset);
            }

            $column_name = self::pop_string($body, $bodyOffset);
            $column_type = self::pop_short($body, $bodyOffset);
            if ($column_type == self::COLUMNTYPE_CUSTOM)
            {
                $column_type = self::pop_string($body, $bodyOffset);
                $column_subtype1 = 0;
                $column_subtype2 = 0;
            } elseif (($column_type == self::COLUMNTYPE_LIST) ||
                ($column_type == self::COLUMNTYPE_SET))
            {
                $column_subtype1 = self::pop_short($body, $bodyOffset);
                if ($column_subtype1 == self::COLUMNTYPE_CUSTOM)
                    $column_subtype1 = self::pop_string($body, $bodyOffset);
                $column_subtype2 = 0;
            } elseif ($column_type == self::COLUMNTYPE_MAP)
            {
                $column_subtype1 = self::pop_short($body, $bodyOffset);
                if ($column_subtype1 == self::COLUMNTYPE_CUSTOM)
                    $column_subtype1 = self::pop_string($body, $bodyOffset);

                $column_subtype2 = self::pop_short($body, $bodyOffset);
                if ($column_subtype2 == self::COLUMNTYPE_CUSTOM)
                    $column_subtype2 = self::pop_string($body, $bodyOffset);
            } else
            {
                $column_subtype1 = 0;
                $column_subtype2 = 0;
            }
            $columns[] = array(
                'keyspace' => $keyspace,
                'table' => $table,
                'name' => $column_name,
                'type' => $column_type,
                'subtype1' => $column_subtype1,
                'subtype2' => $column_subtype2
            );
        }
        return $columns;
    }

    /**
     * Parses a RESULT Rows kind.
     *
     * @param string $body       Frame body to parse.
     * @param string $bodyOffset Offset to start from.
     *
     * @return array Rows with associative array of the records.
     * @access private
     */
    private static function parse_rows($body, $bodyOffset)
    {
        // <metadata><int count><rows_content>
        $columns = self::parse_rows_metadata($body, $bodyOffset);
        $columns_count = count($columns);

        $rows_count = self::pop_int($body, $bodyOffset);

        $retval = array();
        for ($i=0;$i<$rows_count;$i++)
        {
            $row = array();
            foreach ($columns as $col)
            {
                $content = self::pop_bytes($body, $bodyOffset);
                $value = self::unpack_value($content, $col['type'],
                    $col['subtype1'], $col['subtype2']);

                $row[$col['name']] = $value;
            }
            $retval[] = $row;
        }

        return $retval;
    }

    /**
     * Packs a value to its binary form based on a column type. Used for
     * prepared statement.
     *
     * @param mixed $value    Value to pack.
     * @param int   $type     Column type.
     * @param int   $subtype1 Sub column type for list/set or key for map.
     * @param int   $subtype2 Sub column value type for map.
     *
     * @return string Binary form of the value.
     * @access private
     */
    private static function pack_value($value, $type, $subtype1 = 0, $subtype2 = 0)
    {
        switch ($type)
        {
            case self::COLUMNTYPE_CUSTOM:
            case self::COLUMNTYPE_BLOB:
                return self::pack_blob($value);
            case self::COLUMNTYPE_ASCII:
            case self::COLUMNTYPE_TEXT:
            case self::COLUMNTYPE_VARCHAR:
                return $value;
            case self::COLUMNTYPE_BIGINT:
            case self::COLUMNTYPE_COUNTER:
            case self::COLUMNTYPE_TIMESTAMP:
                return self::pack_bigint($value);
            case self::COLUMNTYPE_BOOLEAN:
                return self::pack_boolean($value);
            case self::COLUMNTYPE_DECIMAL:
                return self::pack_decimal($value);
            case self::COLUMNTYPE_DOUBLE:
                return self::pack_double($value);
            case self::COLUMNTYPE_FLOAT:
                return self::pack_float($value);
            case self::COLUMNTYPE_INT:
                return self::pack_int($value);
            case self::COLUMNTYPE_UUID:
            case self::COLUMNTYPE_TIMEUUID:
                return self::pack_uuid($value);
            case self::COLUMNTYPE_VARINT:
                return self::pack_varint($value);
            case self::COLUMNTYPE_INET:
                return self::pack_inet($value);
            case self::COLUMNTYPE_LIST:
            case self::COLUMNTYPE_SET:
                return self::pack_list($value, $subtype1);
            case self::COLUMNTYPE_MAP:
                return self::pack_map($value, $subtype1, $subtype2);
        }

        trigger_error('Unknown column type '.$type);
        return NULL;
    }

    /**
     * Unpacks a value from its binary form based on a column type. Used for
     * parsing rows.
     *
     * @param string $content  Content to unpack.
     * @param int    $type     Column type.
     * @param int    $subtype1 Sub column type for list/set or key for map.
     * @param int    $subtype2 Sub column value type for map.
     *
     * @return mixed Unpacked value.
     * @access private
     */
    private static function unpack_value($content, $type, $subtype1 = 0, $subtype2 = 0)
    {
        if ($content === NULL)
            return NULL;

        switch ($type)
        {
            case self::COLUMNTYPE_CUSTOM:
            case self::COLUMNTYPE_BLOB:
                return self::unpack_blob($content);
            case self::COLUMNTYPE_ASCII:
            case self::COLUMNTYPE_TEXT:
            case self::COLUMNTYPE_VARCHAR:
                return $content;
            case self::COLUMNTYPE_BIGINT:
            case self::COLUMNTYPE_COUNTER:
            case self::COLUMNTYPE_TIMESTAMP:
                return self::unpack_bigint($content);
            case self::COLUMNTYPE_BOOLEAN:
                return self::unpack_boolean($content);
            case self::COLUMNTYPE_DECIMAL:
                return self::unpack_decimal($content);
            case self::COLUMNTYPE_DOUBLE:
                return self::unpack_double($content);
            case self::COLUMNTYPE_FLOAT:
                return self::unpack_float($content);
            case self::COLUMNTYPE_INT:
                return self::unpack_int($content);
            case self::COLUMNTYPE_UUID:
            case self::COLUMNTYPE_TIMEUUID:
                return self::unpack_uuid($content);
            case self::COLUMNTYPE_VARINT:
                return self::unpack_varint($content);
            case self::COLUMNTYPE_INET:
                return self::unpack_inet($content);
            case self::COLUMNTYPE_LIST:
            case self::COLUMNTYPE_SET:
                return self::unpack_list($content, $subtype1);
            case self::COLUMNTYPE_MAP:
                return self::unpack_map($content, $subtype1, $subtype2);
        }

        trigger_error('Unknown column type '.$type);
        return NULL;
    }

    /**
     * Packs a COLUMNTYPE_BLOB value to its binary form.
     *
     * @param string $value Value to pack.
     *
     * @return string Binary form of the value.
     * @access private
     */
    private static function pack_blob($value)
    {
        if (substr($value, 0, 2) == '0x')
            $value = pack('H*', substr($value, 2));
        return $value;
    }

    /**
     * Unpacks a COLUMNTYPE_BLOB value from its binary form.
     *
     * @param string $content Content to unpack.
     *
     * @return string Unpacked value in hexadecimal representation.
     * @access private
     */
    private static function unpack_blob($content, $prefix = '0x')
    {
        $value = unpack('H*', $content);
        if ($value[1])
            $value[1] = $prefix.$value[1];
        return $value[1];
    }

    /**
     * Packs a COLUMNTYPE_BIGINT value to its binary form.
     *
     * @param int $value Value to pack.
     *
     * @return string Binary form of the value.
     * @access private
     */
    private static function pack_bigint($value)
    {
        return self::bin_from_int($value, 8, 1);
    }

    /**
     * Unpacks a COLUMNTYPE_BIGINT value from its binary form.
     *
     * @param string $content Content to unpack.
     *
     * @return int Unpacked value.
     * @access private
     */
    private static function unpack_bigint($content)
    {
        return self::int_from_bin($content, 0, 8, 1);
    }

    /**
     * Packs a COLUMNTYPE_BOOLEAN value to its binary form.
     *
     * @param boolean $value Value to pack.
     *
     * @return string Binary form of the value.
     * @access private
     */
    private static function pack_boolean($value)
    {
        if ($value === NULL)
            return '';
        if ($value == TRUE)
            return chr(1);
        else
            return chr(0);
    }

    /**
     * Unpacks a COLUMNTYPE_BOOLEAN value from its binary form.
     *
     * @param string $content Content to unpack.
     *
     * @return bool Unpacked value.
     * @access private
     */
    private static function unpack_boolean($content)
    {
        if (strlen($content) > 0)
        {
            $c = ord($content[0]);
            if ($c == 1)
                return TRUE;
            elseif ($c == 0)
                return FALSE;
            else
                return NULL;
        }

        return NULL;
    }

    /**
     * Packs a COLUMNTYPE_DECIMAL value to its binary form.
     *
     * @param decimal $value Value to pack.
     *
     * @return string Binary form of the value.
     * @access private
     */
    private static function pack_decimal($value)
    {
        // Based on http://docs.oracle.com/javase/7/docs/api/java/math/BigDecimal.html

        // Find the scale
        $value1 = abs($value);
        $positiveScale = 0;
        while (floor($value1) && (fmod($value1, 10) == 0))
        {
            $value1 /= 10;
            $positiveScale++;
        }

        $value1 = $value;
        $negativeScale = 0;
        while (fmod($value1, 1))
        {
            $value1 *= 10;
            $negativeScale--;
        }

        $scale = $negativeScale-$positiveScale;
        if ($negativeScale)
            $scale = -$negativeScale;
        else
            $scale = -$positiveScale;

        $unscaledValue = $value/pow(10, -$scale);

        return self::pack_int($scale).self::pack_varint($unscaledValue);
    }

    /**
     * Unpacks a COLUMNTYPE_DECIMAL value from its binary form.
     *
     * @param string $content Content to unpack.
     *
     * @return decimal Unpacked value.
     * @access private
     */
    private static function unpack_decimal($content)
    {
        // Based on http://docs.oracle.com/javase/7/docs/api/java/math/BigDecimal.html

        $len = strlen($content);
        if ($len < 5)
            return 0;

        $data = unpack('N', $content);
        $scale = $data[1];
        $unscaledValue = self::unpack_varint(substr($content, 4));

        return $unscaledValue * pow(10, -$scale);
    }

    /**
     * Packs a COLUMNTYPE_DOUBLE value to its binary form.
     *
     * @param double $value Value to pack.
     *
     * @return string Binary form of the value.
     * @access private
     */
    private static function pack_double($value)
    {
        $littleEndian = pack('d', $value);
        $retval = '';
        for ($i=7;$i>=0;$i--)
            $retval .= $littleEndian[$i];
        return $retval;
    }

    /**
     * Unpacks a COLUMNTYPE_DOUBLE value from its binary form.
     *
     * @param string $content Content to unpack.
     *
     * @return double Unpacked value.
     * @access private
     */
    private static function unpack_double($content)
    {
        $bigEndian = '';
        for ($i=7;$i>=0;$i--)
            $bigEndian .= $content[$i];

        $value = unpack('d', $bigEndian);
        return $value[1];
    }

    /**
     * Packs a COLUMNTYPE_FLOAT value to its binary form.
     *
     * @param float $value Value to pack.
     *
     * @return string Binary form of the value.
     * @access private
     */
    private static function pack_float($value)
    {
        $littleEndian = pack('f', $value);
        $retval = '';
        for ($i=3;$i>=0;$i--)
            $retval .= $littleEndian[$i];
        return $retval;
    }

    /**
     * Unpacks a COLUMNTYPE_FLOAT value from its binary form.
     *
     * @param string $content Content to unpack.
     *
     * @return float Unpacked value.
     * @access private
     */
    private static function unpack_float($content)
    {
        $bigEndian = '';
        for ($i=3;$i>=0;$i--)
            $bigEndian .= $content[$i];

        $value = unpack('f', $bigEndian);
        return $value[1];
    }

    /**
     * Packs a COLUMNTYPE_INT value to its binary form.
     *
     * @param int $value Value to pack.
     *
     * @return string Binary form of the value.
     * @access private
     */
    private static function pack_int($value)
    {
        return self::bin_from_int($value, 4, 1);
    }

    /**
     * Unpacks a COLUMNTYPE_INT value from its binary form.
     *
     * @param string $content Content to unpack.
     *
     * @return int Unpacked value.
     * @access private
     */
    private static function unpack_int($content)
    {
        return self::int_from_bin($content, 0, 4, 1);
    }

    /**
     * Packs a COLUMNTYPE_UUID value to its binary form.
     *
     * @param string $value Value to pack.
     *
     * @return string Binary form of the value.
     * @access private
     */
    private static function pack_uuid($value)
    {
        return pack('H*', str_replace('-', '', $value));
    }

    /**
     * Unpacks a COLUMNTYPE_UUID value from its binary form.
     *
     * @param string $content Content to unpack.
     *
     * @return string Unpacked value.
     * @access private
     */
    private static function unpack_uuid($content)
    {
        $value = unpack('H*', $content);
        if ($value[1])
        {
            return substr($value[1], 0, 8).'-'.substr($value[1], 8, 4).'-'.
                   substr($value[1], 12, 4).'-'.substr($value[1], 16, 4).'-'.
                   substr($value[1], 20);
        }

        return NULL;
    }

    /**
     * Packs a COLUMNTYPE_VARINT value to its binary form.
     *
     * @param int $value Value to pack.
     *
     * @return string Binary form of the value.
     * @access private
     */
    private static function pack_varint($content)
    {
        return self::bin_from_int($content, 0xFFFF, 1);
    }

    /**
     * Unpacks a COLUMNTYPE_VARINT value from its binary form.
     *
     * @param string $content Content to unpack.
     *
     * @return int Unpacked value.
     * @access private
     */
    private static function unpack_varint($content)
    {
        return self::int_from_bin($content, 0, strlen($content), 1);
    }

    /**
     * Packs a COLUMNTYPE_INET value to its binary form.
     *
     * @param string $value Value to pack.
     *
     * @return string Binary form of the value.
     * @access private
     */
    private static function pack_inet($value)
    {
        return inet_pton($value);
    }

    /**
     * Unpacks a COLUMNTYPE_INET value from its binary form.
     *
     * @param string $content Content to unpack.
     *
     * @return int Unpacked value.
     * @access private
     */
    private static function unpack_inet($content)
    {
        return inet_ntop($content);
    }

    /**
     * Packs a COLUMNTYPE_LIST value to its binary form.
     *
     * @param array $value   Value to pack.
     * @param int   $subtype Values' Column type.
     *
     * @return string Binary form of the value.
     * @access private
     */
    private static function pack_list($value, $subtype)
    {
        $retval = self::pack_short(count($value));

        foreach ($value as $item)
        {
            $itemPacked = self::pack_value($item, $subtype);
            $retval .= self::pack_string($itemPacked);
        }

        return $retval;
    }

    /**
     * Unpacks a COLUMNTYPE_LIST value from its binary form.
     *
     * @param string $content Content to unpack.
     * @param int    $subtype Values' Column type.
     *
     * @return array Unpacked value.
     * @access private
     */
    private static function unpack_list($content, $subtype)
    {
        $contentOffset = 0;
        $itemsCount = self::pop_short($content, $contentOffset);
        $retval = array();
        for (;$itemsCount;$itemsCount--)
        {
            $subcontent = self::pop_string($content, $contentOffset);
            $retval[] = self::unpack_value($subcontent, $subtype);
        }

        return $retval;
    }

    /**
     * Packs a COLUMNTYPE_MAP value to its binary form.
     *
     * @param array $value    Value to pack.
     * @param int   $subtype1 Keys' column type.
     * @param int   $subtype2 Values' column type.
     *
     * @return string Binary form of the value.
     * @access private
     */
    private static function pack_map($value, $subtype1, $subtype2)
    {
        $retval = self::pack_short(count($value));

        foreach ($value as $key=>$item)
        {
            $keyPacked = self::pack_value($key, $subtype1);
            $itemPacked = self::pack_value($item, $subtype2);
            $retval .= self::pack_string($keyPacked).
                self::pack_string($itemPacked);
        }

        return $retval;
    }

    /**
     * Unpacks a COLUMNTYPE_MAP value from its binary form.
     *
     * @param string $content  Content to unpack.
     * @param int    $subtype1 Keys' column type.
     * @param int    $subtype2 Values' column type.
     *
     * @return array Unpacked value.
     * @access private
     */
    private static function unpack_map($content, $subtype1, $subtype2)
    {
        $contentOffset = 0;
        $itemsCount = self::pop_short($content, $contentOffset);
        $retval = array();
        for (;$itemsCount;$itemsCount--)
        {
            $subKeyRaw = self::pop_string($content, $contentOffset);
            $subValueRaw = self::pop_string($content, $contentOffset);

            $subKey = self::unpack_value($subKeyRaw, $subtype1);
            $subValue = self::unpack_value($subValueRaw, $subtype2);
            $retval[$subKey] = $subValue;
        }

        return $retval;
    }

    /**
     * Pops a [bytes] value from the body, starting from the offset, and
     * advancing it in the process.
     *
     * @param string $body    Content's body.
     * @param string &$offset Offset to start from.
     *
     * @return string Bytes content.
     * @access private
     */
    private static function pop_bytes($body, &$offset)
    {
        $string_length = self::int_from_bin($body, $offset, 4, 0);

        if ($string_length == 0xFFFFFFFF)
        {
            $actual_length = 0;
            $retval = NULL;
        } else
        {
            $actual_length = $string_length;
            $retval = substr($body, $offset+4, $actual_length);
            $offset += $actual_length+4;
        }

        return $retval;
    }

    /**
     * Pops a [string] value from the body, starting from the offset, and
     * advancing it in the process.
     *
     * @param string $body    Content's body.
     * @param string &$offset Offset to start from.
     *
     * @return string String content.
     * @access private
     */
    private static function pop_string($body, &$offset)
    {
        $string_length = unpack('n', substr($body, $offset, 2));
        if ($string_length[1] == 0xFFFF)
        {
            $offset += 2;
            return NULL;
        }
        $retval = substr($body, $offset+2, $string_length[1]);
        $offset += $string_length[1]+2;
        return $retval;
    }

    /**
     * Pops a [int] value from the body, starting from the offset, and
     * advancing it in the process.
     *
     * @param string $body    Content's body.
     * @param string &$offset Offset to start from.
     *
     * @return int Int content.
     * @access private
     */
    private static function pop_int($body, &$offset)
    {
        $retval = self::int_from_bin($body, $offset, 4, 1);
        $offset += 4;
        return $retval;
    }

    /**
     * Pops a [short] value from the body, starting from the offset, and
     * advancing it in the process.
     *
     * @param string $body    Content's body.
     * @param string &$offset Offset to start from.
     *
     * @return short Short content.
     * @access private
     */
    private static function pop_short($body, &$offset)
    {
        $retval = self::int_from_bin($body, $offset, 2, 1);
        $offset += 2;
        return $retval;
    }

    /**
     * Packs an outgoing frame.
     *
     * @param int    $opcode   Frame's opcode.
     * @param string $body     Frame's body.
     * @param int    $response Frame's response flag.
     *
     * @return string Frame's content.
     * @access private
     */
    private static function pack_frame($opcode, $body = '', $response = 0)
    {
        $version = ($response << 0x07) | 1;
        $flags = 0;
        $stream = 0;
        $opcode = $opcode;

        $frame = pack('CCCCNa*', $version, $flags, $stream,
            $opcode, strlen($body), $body);

        return $frame;
    }

    /**
     * Packs a [long string] notation (section 3)
     *
     * @param int $data String content.
     *
     * @return string Data content.
     * @access private
     */
    private static function pack_long_string($data)
    {
        return pack('Na*', strlen($data), $data);
    }

    /**
     * Packs a [string] notation (section 3)
     *
     * @param int $data String content.
     *
     * @return string Data content.
     * @access private
     */
    private static function pack_string($data)
    {
        return pack('na*', strlen($data), $data);
    }

    /**
     * Packs a [short] notation (section 3)
     *
     * @param int $data Short content.
     *
     * @return string Data content.
     * @access private
     */
    private static function pack_short($data)
    {
        return chr($data >> 0x08).chr($data & 0xFF);
    }

    /**
     * Packs a [string map] notation (section 3)
     *
     * @param array $dataArr Associative array of the map.
     *
     * @return string Data content.
     * @access private
     */
    private static function pack_string_map($dataArr)
    {
        $retval = pack('n', count($dataArr));
        foreach ($dataArr as $key=>$value)
            $retval .= self::pack_string($key).self::pack_string($value);
        return $retval;
    }

    /**
     * Converts binary format to a varint.
     *
     * @param string  $data   Binary content.
     * @param int     $offset Starting data offset.
     * @param int     $length Data length.
     * @param boolean $signed Whether the returned data can be signed.
     *
     * @return int Parsed varint.
     * @access private
     */
    private static function int_from_bin($data, $offset, $length, $signed = false)
    {
        $len = strlen($data);

        if ((!$length) || ($offset >= $len))
            return 0;

        $signed = $signed && (ord($data[$offset]) & 0x80);

        $value = 0;
        for ($i=0;$i<$length;$i++)
        {
            $v = ord($data[$i+$offset]);
            if ($signed)
                $v ^= 0xFF;
            $value = $value*256 + $v;
        }

        if ($signed)
            $value = -($value+1);

        return $value;
    }

    /**
     * Converts varint to its binary format.
     *
     * @param int     $value  Binary content.
     * @param int     $offset Starting data offset.
     * @param int     $length Data length.
     * @param boolean $signed Whether the returned data can be signed.
     *
     * @return String Binary content.
     * @access private
     */
    private static function bin_from_int($value, $length, $signed = false)
    {
        $negative = (($signed) && ($value < 0));
        if ($negative)
            $value = -($value+1);

        $retval = '';
        for ($i=0;$i<$length;$i++)
        {
            $v = $value % 256;
            if ($negative)
                $v ^= 0xFF;
            $retval = chr($v).$retval;
            $value = floor($value/256);

            if (($length == 0xFFFF) && ($value == 0))
                break;
        }

        return $retval;
    }
}
