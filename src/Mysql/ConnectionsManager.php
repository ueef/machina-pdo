<?php
declare(strict_types=1);

namespace Ueef\Machina\Pdo\Mysql;

use PDO;
use PDOException;
use Ueef\Machina\Pdo\Interfaces\ConnectionsManagerInterface;
use Ueef\Machina\Pdo\Mysql\Exceptions\ConnectionErrorException;
use Ueef\Machina\Pdo\Mysql\Exceptions\TooManyConnectionsException;

class ConnectionsManager implements ConnectionsManagerInterface
{
    /** @var int */
    private $limit = 10;

    /** @var int */
    private $idle_limit = 2;

    /** @var string */
    private $dsn;

    /** @var string */
    private $user;

    /** @var string */
    private $password;

    /** @var PDO[] */
    private $connections = [];

    /** @var PDO[] */
    private $connections_idle = [];


    public function __construct(array $parameters = [])
    {
        $parameters += [
            'dsn' => $this->buildDsn($parameters),
        ];

        foreach ($parameters as $key => $value) {
            if (property_exists($this, $key)) {
                $this->{$key} = $value;
            }
        }
    }

    public function ping()
    {
        foreach ($this->connections as $connection) {
            $connection->exec("DO 0");
        }
    }

    public function acquire(): PDO
    {
        if ($this->connections_idle) {
            return array_shift($this->connections_idle);
        }

        if (count($this->connections) >= $this->limit) {
            throw new TooManyConnectionsException(["too many connections. connections limit is %s", $this->limit]);
        }

        $connection = $this->createConnection();
        $this->connections[] = $connection;

        return $connection;
    }

    public function release(PDO $connection)
    {
        if (count($this->connections_idle) < $this->idle_limit) {
            $this->connections_idle[] = $connection;
        } else {
            $this->removeConnection($connection);
        }
    }

    private function createConnection(): PDO
    {
        try {
            $connection = new PDO($this->dsn, $this->user, $this->password);
        } catch (PDOException $e) {
            throw new ConnectionErrorException($e->getMessage(), 0, $e);
        }

        $connection->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

        return $connection;
    }

    private function removeConnection(PDO $connection)
    {
        $this->connections = array_filter($this->connections, function ($v) use ($connection) {
            return $connection !== $v;
        });
        $this->connections = array_values($this->connections);
    }

    private function buildDsn(array $parameters): string
    {
        $parameters = [
            'host' => $parameters['host'] ?? '',
            'port' => $parameters['port'] ?? '',
            'dbname' => $parameters['dbname'] ??  '',
            'charset' => $parameters['charset'] ?? '',
            'unix_socket' => $parameters['unix_socket'] ?? '',
        ];

        $dsn = [];
        foreach ($parameters as $key => &$value) {
            if ($value) {
                $dsn[] = $key . '=' . $value;
            }
        }

        return 'mysql:' . join(';', $dsn);
    }
}