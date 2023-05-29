/* eslint-disable-next-line import/no-extraneous-dependencies */
import RedshiftData from 'aws-sdk/clients/redshiftdata';
import { WorkGroupProps } from './types';

const redshiftData = new RedshiftData();

export async function executeStatement(statement: string, workGroupProps: WorkGroupProps): Promise<void> {
  const executedStatement = await redshiftData.executeStatement({
    WorkgroupName: workGroupProps.workGroupName,
    Database: workGroupProps.databaseName,
    Sql: statement,
  }).promise();

  console.log('Executing Statement: ', statement);
  
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
  const statement = await redshiftData.describeStatement({ Id: statementId }).promise();
  if (statement.Status !== 'FINISHED' && statement.Status !== 'FAILED' && statement.Status !== 'ABORTED') {
    return waitForStatementComplete(statementId);
  } else if (statement.Status === 'FINISHED') {
    return;
  } else {
    throw new Error(`Statement status was ${statement.Status}: ${statement.Error}`);
  }
}