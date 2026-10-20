package dev.ingest.sink.postgres;

import dev.ingest.core.ColumnType;

/**
 * Sealed hierarchy representing the outcome of evaluating an incoming record field against the stored schema.
 */
public sealed interface SchemaDecision
        permits SchemaDecision.NoChange,
                SchemaDecision.AddColumn,
                SchemaDecision.WidenColumn,
                SchemaDecision.SendToDlq {

    record NoChange() implements SchemaDecision {}

    record AddColumn(String columnName, ColumnType columnType) implements SchemaDecision {}

    record WidenColumn(String columnName, ColumnType oldType, ColumnType newType) implements SchemaDecision {}

    record SendToDlq(String columnName, String reasonCode, String reasonDetail) implements SchemaDecision {}
}
