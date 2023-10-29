/* eslint-disable-next-line import/no-extraneous-dependencies */
import { NamespaceProps } from './types';
import { RedshiftDataClient, DescribeStatementCommand, ExecuteStatementCommand } from '@aws-sdk/client-redshift-data';

const redshiftData = new RedshiftDataClient();

export async function executeStatement(statement: string, namespaceProps: NamespaceProps): Promise<void> {
  const executedStatement = await redshiftData.send(
    new ExecuteStatementCommand({
      WorkgroupName: namespaceProps.workGroupName,
      Database: namespaceProps.databaseName,
      SecretArn: namespaceProps.adminUserArn,
      Sql: statement,
    })
  )
  
  if (!executedStatement.Id) {
    throw new Error('Service error: Statement execution did not return a statement ID');
  }
  await waitForStatementComplete(executedStatement.Id);
}

const waitTimeout = 100;
async function waitForStatementComplete(statementId: string): Promise<void> {
  await new Promise((resolve: (value: void) => void) => {
    setTimeout(() => resolve(), waitTimeout);
  });
  const statement = await redshiftData.send(
    new DescribeStatementCommand({ Id: statementId })
  )

  if (statement.Status !== 'FINISHED' && statement.Status !== 'FAILED' && statement.Status !== 'ABORTED') {
    return waitForStatementComplete(statementId);
  } else if (statement.Status === 'FINISHED') {
    return;
  } else {
    throw new Error(`Statement status was ${statement.Status}: ${statement.Error}`);
  }
}