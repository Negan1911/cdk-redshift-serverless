/* eslint-disable-next-line import/no-unresolved */
import * as AWSLambda from 'aws-lambda';
import { executeStatement } from './redshift-data';
import { WorkGroupProps, TableAndWorkGroupProps, TableSortStyle } from './types';
import { areColumnsEqual, getDistKeyColumn, getSortKeyColumns } from './util';
import { Column } from '../../table';

export async function handler(props: TableAndWorkGroupProps, event: AWSLambda.CloudFormationCustomResourceEvent) {
  const tableNamePrefix = props.tableName.prefix;
  const tableNameSuffix = props.tableName.generateSuffix === 'true' ? `${event.RequestId.substring(0, 8)}` : '';
  const tableColumns = props.tableColumns;
  const tableAndWorkGroupProps = props;
  const useColumnIds = props.useColumnIds;

  if (event.RequestType === 'Create') {
    const tableName = await createTable(tableNamePrefix, tableNameSuffix, tableColumns, tableAndWorkGroupProps);
    return { PhysicalResourceId: tableName };
  } else if (event.RequestType === 'Delete') {
    await dropTable(event.PhysicalResourceId, tableAndWorkGroupProps);
    return;
  } else if (event.RequestType === 'Update') {
    const tableName = await updateTable(
      event.PhysicalResourceId,
      tableNamePrefix,
      tableNameSuffix,
      tableColumns,
      useColumnIds,
      tableAndWorkGroupProps,
      event.OldResourceProperties as TableAndWorkGroupProps,
    );
    return { PhysicalResourceId: tableName };
  } else {
    /* eslint-disable-next-line dot-notation */
    throw new Error(`Unrecognized event type: ${event['RequestType']}`);
  }
}

async function createTable(
  tableNamePrefix: string,
  tableNameSuffix: string,
  tableColumns: Column[],
  tableAndWorkGroupProps: TableAndWorkGroupProps,
): Promise<string> {
  const tableName = tableNamePrefix + tableNameSuffix;
  const tableColumnsString = tableColumns.map(column => `${column.name} ${column.dataType}${getEncodingColumnString(column)}`).join();

  let statement = `CREATE TABLE ${tableName} (${tableColumnsString})`;

  if (tableAndWorkGroupProps.distStyle) {
    statement += ` DISTSTYLE ${tableAndWorkGroupProps.distStyle}`;
  }

  const distKeyColumn = getDistKeyColumn(tableColumns);
  if (distKeyColumn) {
    statement += ` DISTKEY(${distKeyColumn.name})`;
  }

  const sortKeyColumns = getSortKeyColumns(tableColumns);
  if (sortKeyColumns.length > 0) {
    const sortKeyColumnsString = getSortKeyColumnsString(sortKeyColumns);
    statement += ` ${tableAndWorkGroupProps.sortStyle} SORTKEY(${sortKeyColumnsString})`;
  }

  await executeStatement(statement, tableAndWorkGroupProps);

  for (const column of tableColumns) {
    if (column.comment) {
      await executeStatement(`COMMENT ON COLUMN ${tableName}.${column.name} IS '${column.comment}'`, tableAndWorkGroupProps);
    }
  }
  if (tableAndWorkGroupProps.tableComment) {
    await executeStatement(`COMMENT ON TABLE ${tableName} IS '${tableAndWorkGroupProps.tableComment}'`, tableAndWorkGroupProps);
  }

  return tableName;
}

async function dropTable(tableName: string, WorkGroupProps: WorkGroupProps) {
  await executeStatement(`DROP TABLE ${tableName}`, WorkGroupProps);
}

async function updateTable(
  tableName: string,
  tableNamePrefix: string,
  tableNameSuffix: string,
  tableColumns: Column[],
  useColumnIds: boolean,
  tableAndWorkGroupProps: TableAndWorkGroupProps,
  oldResourceProperties: TableAndWorkGroupProps,
): Promise<string> {
  const alterationStatements: string[] = [];

  const oldWorkGroupProps = oldResourceProperties;
  if (tableAndWorkGroupProps.workGroupName !== oldWorkGroupProps.workGroupName || tableAndWorkGroupProps.databaseName !== oldWorkGroupProps.databaseName) {
    return createTable(tableNamePrefix, tableNameSuffix, tableColumns, tableAndWorkGroupProps);
  }

  const oldTableNamePrefix = oldResourceProperties.tableName.prefix;
  if (tableNamePrefix !== oldTableNamePrefix) {
    return createTable(tableNamePrefix, tableNameSuffix, tableColumns, tableAndWorkGroupProps);
  }

  const oldTableColumns = oldResourceProperties.tableColumns;
  const columnDeletions = oldTableColumns.filter(oldColumn => (
    tableColumns.every(column => {
      if (useColumnIds) {
        return oldColumn.id ? oldColumn.id !== column.id : oldColumn.name !== column.name;
      }
      return oldColumn.name !== column.name;
    })
  ));
  if (columnDeletions.length > 0) {
    alterationStatements.push(...columnDeletions.map(column => `ALTER TABLE ${tableName} DROP COLUMN ${column.name}`));
  }

  const columnAdditions = tableColumns.filter(column => {
    return !oldTableColumns.some(oldColumn => {
      if (useColumnIds) {
        return oldColumn.id ? oldColumn.id === column.id : oldColumn.name === column.name;
      }
      return oldColumn.name === column.name;
    });
  }).map(column => `ADD ${column.name} ${column.dataType}`);
  if (columnAdditions.length > 0) {
    alterationStatements.push(...columnAdditions.map(addition => `ALTER TABLE ${tableName} ${addition}`));
  }

  const columnEncoding = tableColumns.filter(column => {
    return oldTableColumns.some(oldColumn => column.name === oldColumn.name && column.encoding !== oldColumn.encoding);
  }).map(column => `ALTER COLUMN ${column.name} ENCODE ${column.encoding || 'AUTO'}`);
  if (columnEncoding.length > 0) {
    alterationStatements.push(`ALTER TABLE ${tableName} ${columnEncoding.join(', ')}`);
  }

  const columnComments = tableColumns.filter(column => {
    return oldTableColumns.some(oldColumn => column.name === oldColumn.name && column.comment !== oldColumn.comment);
  }).map(column => `COMMENT ON COLUMN ${tableName}.${column.name} IS ${column.comment ? `'${column.comment}'` : 'NULL'}`);
  if (columnComments.length > 0) {
    alterationStatements.push(...columnComments);
  }

  if (useColumnIds) {
    const columnNameUpdates = tableColumns.reduce((updates, column) => {
      const oldColumn = oldTableColumns.find(oldCol => oldCol.id && oldCol.id === column.id);
      if (oldColumn && oldColumn.name !== column.name) {
        updates[oldColumn.name] = column.name;
      }
      return updates;
    }, {} as Record<string, string>);
    if (Object.keys(columnNameUpdates).length > 0) {
      alterationStatements.push(...Object.entries(columnNameUpdates).map(([oldName, newName]) => (
        `ALTER TABLE ${tableName} RENAME COLUMN ${oldName} TO ${newName}`
      )));
    }
  }

  const oldDistStyle = oldResourceProperties.distStyle;
  if ((!oldDistStyle && tableAndWorkGroupProps.distStyle) ||
    (oldDistStyle && !tableAndWorkGroupProps.distStyle)) {
    return createTable(tableNamePrefix, tableNameSuffix, tableColumns, tableAndWorkGroupProps);
  } else if (oldDistStyle !== tableAndWorkGroupProps.distStyle) {
    alterationStatements.push(`ALTER TABLE ${tableName} ALTER DISTSTYLE ${tableAndWorkGroupProps.distStyle}`);
  }

  const oldDistKey = getDistKeyColumn(oldTableColumns)?.name;
  const newDistKey = getDistKeyColumn(tableColumns)?.name;
  if ((!oldDistKey && newDistKey ) || (oldDistKey && !newDistKey)) {
    return createTable(tableNamePrefix, tableNameSuffix, tableColumns, tableAndWorkGroupProps);
  } else if (oldDistKey !== newDistKey) {
    alterationStatements.push(`ALTER TABLE ${tableName} ALTER DISTKEY ${newDistKey}`);
  }

  const oldSortKeyColumns = getSortKeyColumns(oldTableColumns);
  const newSortKeyColumns = getSortKeyColumns(tableColumns);
  const oldSortStyle = oldResourceProperties.sortStyle;
  const newSortStyle = tableAndWorkGroupProps.sortStyle;
  if ((oldSortStyle === newSortStyle && !areColumnsEqual(oldSortKeyColumns, newSortKeyColumns))
    || (oldSortStyle !== newSortStyle)) {
    switch (newSortStyle) {
      case TableSortStyle.INTERLEAVED:
        // INTERLEAVED sort key addition requires replacement.
        // https://docs.aws.amazon.com/redshift/latest/dg/r_ALTER_TABLE.html
        return createTable(tableNamePrefix, tableNameSuffix, tableColumns, tableAndWorkGroupProps);

      case TableSortStyle.COMPOUND: {
        const sortKeyColumnsString = getSortKeyColumnsString(newSortKeyColumns);
        alterationStatements.push(`ALTER TABLE ${tableName} ALTER ${newSortStyle} SORTKEY(${sortKeyColumnsString})`);
        break;
      }

      case TableSortStyle.AUTO: {
        alterationStatements.push(`ALTER TABLE ${tableName} ALTER SORTKEY ${newSortStyle}`);
        break;
      }
    }
  }

  const oldComment = oldResourceProperties.tableComment;
  const newComment = tableAndWorkGroupProps.tableComment;
  if (oldComment !== newComment) {
    alterationStatements.push(`COMMENT ON TABLE ${tableName} IS ${newComment ? `'${newComment}'` : 'NULL'}`);
  }

  await Promise.all(alterationStatements.map(statement => executeStatement(statement, tableAndWorkGroupProps)));

  return tableName;
}

function getSortKeyColumnsString(sortKeyColumns: Column[]) {
  return sortKeyColumns.map(column => column.name).join();
}

function getEncodingColumnString(column: Column): string {
  if (column.encoding) {
    return ` ENCODE ${column.encoding}`;
  }
  return '';
}