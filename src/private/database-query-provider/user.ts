import AWSLambda from 'aws-lambda';
import { executeStatement } from './redshift-data';
import { WorkGroupProps } from './types';
import { makePhysicalId } from './util';
import { UserHandlerProps } from '../handler-props';


export async function handler(props: UserHandlerProps & WorkGroupProps, event: AWSLambda.CloudFormationCustomResourceEvent) {
  const username = props.username;
  const workGroupProps = props;

  if (event.RequestType === 'Create') {
    await createUser(username, workGroupProps);
    return { PhysicalResourceId: makePhysicalId(username, workGroupProps, event.RequestId), Data: { username: username } };
  } else if (event.RequestType === 'Delete') {
    await dropUser(username, workGroupProps);
    return;
  } else if (event.RequestType === 'Update') {
    const { replace } = await updateUser(username, workGroupProps, event.OldResourceProperties as UserHandlerProps & WorkGroupProps);
    const physicalId = replace ? makePhysicalId(username, workGroupProps, event.RequestId) : event.PhysicalResourceId;
    return { PhysicalResourceId: physicalId, Data: { username: username } };
  } else {
    /* eslint-disable-next-line dot-notation */
    throw new Error(`Unrecognized event type: ${event['RequestType']}`);
  }
}

async function dropUser(username: string, workGroupProps: WorkGroupProps) {
  await executeStatement(`DROP USER ${username}`, workGroupProps);
}

async function createUser(username: string, workGroupProps: WorkGroupProps) {
  await executeStatement(`CREATE USER ${username} password disable`, workGroupProps);
}

async function updateUser(
  username: string,
  workGroupProps: WorkGroupProps,
  oldResourceProperties: UserHandlerProps & WorkGroupProps,
): Promise<{ replace: boolean }> {
  const oldWorkGroupProps = oldResourceProperties;
  if (workGroupProps.workGroupName !== oldWorkGroupProps.workGroupName || workGroupProps.databaseName !== oldWorkGroupProps.databaseName) {
    await createUser(username, workGroupProps);
    return { replace: true };
  }

  const oldUsername = oldResourceProperties.username;
  
  if (username !== oldUsername) {
    await createUser(username, workGroupProps);
    return { replace: true };
  }

  return { replace: false };
}