import AWSLambda from 'aws-lambda';
import SecretsManager from 'aws-sdk/clients/secretsmanager';
import { executeStatement } from './redshift-data';
import { WorkGroupProps } from './types';
import { makePhysicalId } from './util';
import { UserHandlerProps } from '../handler-props';

const secretsManager = new SecretsManager();

export async function handler(props: UserHandlerProps & WorkGroupProps, event: AWSLambda.CloudFormationCustomResourceEvent) {
  const username = props.username;
  const passwordSecretArn = props.passwordSecretArn;
  const workGroupProps = props;

  if (event.RequestType === 'Create') {
    await createUser(username, passwordSecretArn, workGroupProps);
    return { PhysicalResourceId: makePhysicalId(username, workGroupProps, event.RequestId), Data: { username: username } };
  } else if (event.RequestType === 'Delete') {
    await dropUser(username, workGroupProps);
    return;
  } else if (event.RequestType === 'Update') {
    const { replace } = await updateUser(username, passwordSecretArn, workGroupProps, event.OldResourceProperties as UserHandlerProps & WorkGroupProps);
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

async function createUser(username: string, passwordSecretArn: string, workGroupProps: WorkGroupProps) {
  const password = await getPasswordFromSecret(passwordSecretArn);

  await executeStatement(`CREATE USER ${username} PASSWORD '${password}'`, workGroupProps);
}

async function updateUser(
  username: string,
  passwordSecretArn: string,
  workGroupProps: WorkGroupProps,
  oldResourceProperties: UserHandlerProps & WorkGroupProps,
): Promise<{ replace: boolean }> {
  const oldWorkGroupProps = oldResourceProperties;
  if (workGroupProps.workGroupName !== oldWorkGroupProps.workGroupName || workGroupProps.databaseName !== oldWorkGroupProps.databaseName) {
    await createUser(username, passwordSecretArn, workGroupProps);
    return { replace: true };
  }

  const oldUsername = oldResourceProperties.username;
  const oldPasswordSecretArn = oldResourceProperties.passwordSecretArn;
  const oldPassword = await getPasswordFromSecret(oldPasswordSecretArn);
  const password = await getPasswordFromSecret(passwordSecretArn);

  if (username !== oldUsername) {
    await createUser(username, passwordSecretArn, workGroupProps);
    return { replace: true };
  }

  if (password !== oldPassword) {
    await executeStatement(`ALTER USER ${username} PASSWORD '${password}'`, workGroupProps);
    return { replace: false };
  }

  return { replace: false };
}

async function getPasswordFromSecret(passwordSecretArn: string): Promise<string> {
  const secretValue = await secretsManager.getSecretValue({
    SecretId: passwordSecretArn,
  }).promise();
  const secretString = secretValue.SecretString;
  if (!secretString) {
    throw new Error(`Secret string for ${passwordSecretArn} was empty`);
  }
  const { password } = JSON.parse(secretString);

  return password;
}