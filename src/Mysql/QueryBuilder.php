<?php
declare(strict_types=1);

namespace Ueef\Machina\Pdo\Mysql;

use Ueef\Machina\Pdo\Mysql\Exceptions\QueryBuilderErrorException;
use Ueef\Machina\Interfaces\FilterInterface;
use Ueef\Machina\Interfaces\OrderInterface;

class QueryBuilder
{
    public function select(&$binds, string $table, array $select, array $filters = [], array $orders = [], int $limit = 0, int $offset = 0)
    {
        $binds = [];
        $query = 'SELECT ' . join(',', $select) . ' FROM `' . $table . '`' . $this->parseCondition($binds, $filters, $orders, $limit, $offset);

        return $query;
    }

    public function insert(&$binds, string $table, array $keys, array ...$rows): string
    {
        $binds = [];
        foreach ($rows as &$row) {
            foreach ($row as $index => &$value) {
                $binds[] = [$keys[$index], $value];
                $value = '?';
            }
            $row = '(' . join(',', $row) . ')';
        }

        return 'INSERT INTO `' . $table . '` (`' . join('`,`', $keys) . '`) VALUES ' . join(',', $rows);
    }

    public function update(&$binds, string $table, array $values, array $filters = [], array $orders = [], int $limit = 0, int $offset = 0): string
    {
        $binds = [];
        foreach ($values as $key => &$value) {
            $binds[] = [$key, $value];
            $value = '`' . $key . '` = ?';
        }

        return 'UPDATE `' . $table . '` SET ' . join(',', $values) . $this->parseCondition($binds, $filters, $orders, $limit, $offset);
    }

    public function delete(&$binds, string $table, array $filters = [], array $orders = [], int $limit = 0, int $offset = 0): string
    {
        $binds = [];
        $query = 'DELETE FROM `' . $table . '`' . $this->parseCondition($binds, $filters, $orders, $limit, $offset);

        return $query;
    }

    private function parseCondition(&$binds, array $filters = [], array $orders = [], int $limit = 0, int $offset = 0): string
    {
        foreach ($filters as &$filter) {
            $filter = $this->parseFilter($binds, ...$filter);
        }
        foreach ($orders as $property => &$direction) {
            $direction = '`' . $property . '` ' . (OrderInterface::ASC == $direction ? 'ASC' : 'DESC');
        }

        return (
            ($filters ? ' WHERE ' . join(' AND ', $filters) : '') .
            ($orders ? ' ORDER BY ' . join(', ', $orders) : '') .
            ($limit ? ' LIMIT ' . $limit : '') .
            ($offset ? ' OFFSET ' . $offset : '')
        );
    }

    private function parseFilter(&$binds, string $operator, ...$operands)
    {
        switch (count($operands)) {
            case 1:
                return $this->parseFilterConjunction($binds, $operator, ...$operands);
                break;
            case 2:
                return $this->parseFilterCondition($binds, $operator, ...$operands);
                break;
            default:
                throw new QueryBuilderErrorException("wrong number of operands in %s", array_slice(func_get_args(), 1));
        }
    }

    private function parseFilterCondition(&$binds, string $operator, string $key, $operand): string
    {
        switch ($operator) {
            case FilterInterface::EQ:
                if (null === $operand) {
                    $operator = 'is';
                } elseif (is_array($operand)) {
                    $operator = 'in';
                } else {
                    $operator = '=';
                }
                break;
            case FilterInterface::NE:
                if (null === $operand) {
                    $operator = 'is not';
                } elseif (is_array($operand)) {
                    $operator = 'not in';
                } else {
                    $operator = '!=';
                }
                break;
            case FilterInterface::GT:
                $operator = '>';
                if (is_array($operand)) {
                    $operand = max($operand);
                }
                break;
            case FilterInterface::LT:
                $operator = '<';
                if (is_array($operand)) {
                    $operand = min($operand);
                }
                break;
            case FilterInterface::GE:
                $operator = '>=';
                if (is_array($operand)) {
                    $operand = max($operand);
                }
                break;
            case FilterInterface::LE:
                $operator = '<=';
                if (is_array($operand)) {
                    $operand = min($operand);
                }
                break;
            default:
                throw new QueryBuilderErrorException(["unsupported operator \"%s\" in %s", $operator, array_slice(func_get_args(), 1)]);
        }

        if (is_array($operand)) {
            foreach ($operand as &$value) {
                $binds[] = [$key, $value];
                $value = '?';
            }
            $operand = '(' . join(',', $operand) . ')';
        } else {
            $binds[] = [$key, $operand];
            $operand = '?';
        }

        return '`' . $key . '` ' . $operator . ' ' . $operand;
    }

    private function parseFilterConjunction(&$binds, string $operator, array $operand): string
    {
        switch ($operator) {
            case FilterInterface::OR:
                $operator = 'OR';
                break;
            case FilterInterface::XOR:
                $operator = 'XOR';
                break;
            case FilterInterface::AND:
                $operator = 'AND';
                break;
            default:
                throw new QueryBuilderErrorException(["unsupported operator \"%s\" in %s", $operator, array_slice(func_get_args(), 1)]);
        }

        foreach ($operand as &$filter) {
            $filter = $this->parseFilter($binds, ...$filter);
        }

        if ($operand) {
            return '(' . join(' ' . $operator . ' ', $operand) . ')';
        }

        return '';
    }
}