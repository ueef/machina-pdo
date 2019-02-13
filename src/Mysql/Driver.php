<?php
declare(strict_types=1);

namespace Ueef\Machina\Pdo\Mysql;

use PDO;
use PDOStatement;
use PDOException;
use Ueef\Machina\Exceptions\DriverException;
use Ueef\Encoder\Interfaces\EncoderInterface;
use Ueef\Machina\Interfaces\DriverInterface;
use Ueef\Machina\Interfaces\MetadataInterface;
use Ueef\Machina\Interfaces\PropertyInterface;
use Ueef\Machina\Interfaces\LockableDriverInterface;
use Ueef\Machina\Interfaces\TransactionalDriverInterface;
use Ueef\Machina\Pdo\Mysql\Exceptions\QueryErrorException;
use Ueef\Machina\Pdo\Mysql\Transactions\RealTransaction;
use Ueef\Machina\Pdo\Mysql\Transactions\SavepointTransaction;
use Ueef\Machina\Pdo\Interfaces\TransactionInterface;
use Ueef\Machina\Pdo\Interfaces\ConnectionsManagerInterface;

class Driver implements DriverInterface, TransactionalDriverInterface, LockableDriverInterface
{
    /** @var EncoderInterface */
    private $encoder;

    /** @var PDO */
    private $connection;

    /** @var TransactionInterface[] */
    private $transactions = [];

    /** @var QueryBuilder */
    private $query_builder;


    public function __construct(ConnectionsManagerInterface $connectionManager, EncoderInterface $encoder)
    {
        $this->encoder = $encoder;
        $this->connection = $connectionManager->acquire();
        $this->query_builder = new QueryBuilder();
    }

    public function find(MetadataInterface $metadata, array $filters = [], array $orders = [], int $limit = 0, int $offset = 0): array
    {
        $properties = $metadata->getProperties();

        $select = [];
        foreach ($properties as $key => $property) {
            $select[] = '`' . $key . '`';
        }

        $query = $this->query_builder->select($binds, $metadata->getSource(), $select, $filters, $orders, $limit, $offset);
        $query = $this->execute($metadata, $query, $binds);

        $counter = 0;
        $columns = [];
        foreach ($properties as $key => $property) {
            switch ($property->getType()) {
                case PropertyInterface::TYPE_INT:
                    $boundType = PDO::PARAM_INT;
                    break;
                case PropertyInterface::TYPE_BOOL:
                    $boundType = PDO::PARAM_BOOL;
                    break;
                case PropertyInterface::TYPE_STR:
                case PropertyInterface::TYPE_FLOAT:
                case PropertyInterface::TYPE_ARRAY:
                    $boundType = PDO::PARAM_STR;
                    break;
                default:
                    throw new DriverException(["the type of property %s is unsupported", $property->getType()]);
            }

            $query->bindColumn(++$counter, $columns[$key], $boundType);
        }

        $items = [];
        while ($query->fetch(PDO::FETCH_BOUND)) {
            $item = [];
            foreach ($columns as $key => $value) {
                switch ($properties[$key]->getType()) {
                    case PropertyInterface::TYPE_FLOAT:
                        $item[$key] = (float) $value;
                        break;
                    case PropertyInterface::TYPE_ARRAY:
                        $item[$key] = $this->encoder->decode($value);
                        break;
                    default:
                        $item[$key] = $value;
                }
            }
            $items[] = $item;
        }

        return $items;
    }

    public function count(MetadataInterface $metadata, array $filters = []): int
    {
        $query = $this->query_builder->select($binds, $metadata->getSource(), ['count(*)'], $filters, [], 0, 0);
        $query = $this->execute($metadata, $query, $binds);

        return (int) $query->fetchColumn();
    }

    public function insert(MetadataInterface $metadata, array &$rows): void
    {
        if (MetadataInterface::GENERATION_STRATEGY_AUTO == $metadata->getGenerationStrategy()) {
            foreach ($rows as &$item) {
                $this->executeInsert($metadata, $item);

                foreach ($metadata->getGeneratedProperties() as $key => $property) {
                    if (isset($item[$key])) {
                        break;
                    }

                    $value = $this->connection->lastInsertId();
                    switch ($property->getType()) {
                        case PropertyInterface::TYPE_INT:
                            $item[$key] = (int) $value;
                            break;
                        case PropertyInterface::TYPE_STR:
                            $item[$key] = $value;
                            break;
                        default:
                            throw new DriverException(["cannot generate the value of type \"%s\" automatically", $property->getType()]);
                            break;
                    }
                    break;
                }
            }
        } else {
            $this->executeInsert($metadata, ...$rows);
        }
    }

    public function update(MetadataInterface $metadata, array $values, array $filters = [], array $orders = [], int $limit = 0, int $offset = 0): void
    {
        $query = $this->query_builder->update($binds, $metadata->getSource(), $values, $filters, $orders, $limit, $offset);
        $this->execute($metadata, $query, $binds);
    }

    public function delete(MetadataInterface $metadata, array $filters = [], array $orders = [], int $limit = 0, int $offset = 0): void
    {
        $query = $this->query_builder->delete($binds, $metadata->getSource(), $filters, $orders, $limit, $offset);
        $this->execute($metadata, $query, $binds);
    }

    public function lock(MetadataInterface $metadata, string $resource, bool $wait = true): bool
    {
        if ($wait) {
            $timeout = -1;
        } else {
            $timeout = 0;
        }
        $query = $this->connection->prepare("SELECT GET_LOCK(concat(database(), '.', ?, '.', ?), ?)");
        $query->bindValue(1, $metadata->getSource(), PDO::PARAM_STR);
        $query->bindValue(2, $resource, PDO::PARAM_STR);
        $query->bindValue(3, $timeout, PDO::PARAM_INT);

        try {
            $success = $query->execute();
        } catch (PDOException $e) {
            return false;
        }

        if ($success) {
            $success = (bool) $query->fetchColumn(0);
        }

        return $success;
    }

    public function unlock(MetadataInterface $metadata, string $resource): bool
    {
        $query = $this->connection->prepare("SELECT RELEASE_LOCK(concat(database(), '.', ?, '.', ?))");
        $query->bindValue(1, $metadata->getSource(), PDO::PARAM_STR);
        $query->bindValue(2, $resource, PDO::PARAM_STR);

        try {
            $success = $query->execute();
        } catch (PDOException $e) {
            return false;
        }

        if ($success) {
            $success = (bool) $query->fetchColumn(0);
        }

        return $success;
    }

    public function begin(): void
    {
        if ($this->transactions) {
            $transaction = new SavepointTransaction($this->connection);
        } else {
            $transaction = new RealTransaction($this->connection);
        }

        $this->transactions[] = $transaction;
    }

    public function commit(): void
    {
        if ($this->transactions) {
            array_pop($this->transactions)->commit();
        }
    }

    public function rollback(): void
    {
        if ($this->transactions) {
            array_pop($this->transactions)->rollback();
        }
    }

    private function execute(MetadataInterface $metadata, string $query, array $binds): PDOStatement
    {
        $query = $this->connection->prepare($query);

        $properties = $metadata->getProperties();
        foreach ($binds as $index => [$key, $value]) {
            if (!isset($properties[$key])) {
                throw new DriverException(["the property %s is undefined", $key]);
            }

            $property = $properties[$key];
            $property->validate($value);

            switch ($properties[$key]->getType()) {
                case PropertyInterface::TYPE_INT:
                    $type = PDO::PARAM_INT;
                    break;
                case PropertyInterface::TYPE_STR:
                    $type = PDO::PARAM_STR;
                    break;
                case PropertyInterface::TYPE_BOOL:
                    $type = PDO::PARAM_BOOL;
                    break;
                case PropertyInterface::TYPE_FLOAT:
                    $type = PDO::PARAM_STR;
                    break;
                case PropertyInterface::TYPE_ARRAY:
                    $value = $this->encoder->encode($value);
                    $type = PDO::PARAM_STR;
                    break;
                default:
                    throw new DriverException(["the type of property %s is incompatible", $property->getType()]);
            }

            $query->bindValue($index+1, $value, $type);
        }

        try {
            $query->execute();
        } catch (PDOException $e) {
            throw new QueryErrorException($e->getMessage(), 0, $e);
        }

        return $query;
    }

    private function executeInsert(MetadataInterface $metadata, array &...$items): void
    {
        if (MetadataInterface::GENERATION_STRATEGY_CUSTOM == $metadata->getGenerationStrategy()) {
            $metadata->getGenerator()->generate($metadata, $items);
        }

        $groupsKeys = [];
        $groupsRows = [];
        foreach ($items as $item) {
            ksort($item);

            $row = [];
            $keys = [];
            foreach ($item as $key => $value) {
                if (null === $value) {
                    continue;
                }

                $row[] = $value;
                $keys[] = $key;
            }

            $groupKey = join(';', $keys);
            if (!isset($groupsKeys[$groupKey])) {
                $groupsKeys[$groupKey] = $keys;
            }
            $groupsRows[$groupKey][] = $row;
        }

        foreach ($groupsRows as $groupKey => &$rows) {
            $query = $this->query_builder->insert($binds, $metadata->getSource(), $groupsKeys[$groupKey], ...$rows);
            $this->execute($metadata, $query, $binds);
        }
    }
}