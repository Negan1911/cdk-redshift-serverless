import * as path from 'path';
import * as cdk from 'aws-cdk-lib';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as customresources from 'aws-cdk-lib/custom-resources';
import { Construct } from 'constructs';
import { DatabaseQueryHandlerProps } from './handler-props';
import { DatabaseOptions } from '../database-options';
import { builtInCustomResourceNodeRuntime } from 'aws-cdk-lib/custom-resources';

export interface DatabaseQueryProps<HandlerProps> extends DatabaseOptions {
  readonly handler: string;
  readonly properties: HandlerProps;
  /**
   * The policy to apply when this resource is removed from the application.
   *
   * @default cdk.RemovalPolicy.Destroy
   */
  readonly removalPolicy?: cdk.RemovalPolicy;
}

export class DatabaseQuery<HandlerProps> extends Construct implements iam.IGrantable {
  readonly grantPrincipal: iam.IPrincipal;
  readonly ref: string;

  private readonly resource: cdk.CustomResource;

  constructor(scope: Construct, id: string, props: DatabaseQueryProps<HandlerProps>) {
    super(scope, id);

    const handler = new lambda.SingletonFunction(this, 'Handler', {
      code: lambda.Code.fromAsset(path.join(__dirname, 'database-query-provider'), {
        exclude: ['*.ts'],
      }),
      runtime: builtInCustomResourceNodeRuntime(this),
      handler: 'index.handler',
      timeout: cdk.Duration.minutes(1),
      uuid: '3de5bea7-27da-4796-8662-5efb56431b5f',
      lambdaPurpose: 'Query Redshift Database',
    });

    handler.addToRolePolicy(new iam.PolicyStatement({
      actions: ['redshift-serverless:*'],
      resources: ['*']
    }));

    handler.addToRolePolicy(new iam.PolicyStatement({
      actions: ['redshift-data:DescribeStatement', 'redshift-data:ExecuteStatement'],
      resources: ['*'],
    }));

    const provider = new customresources.Provider(this, 'Provider', {
      onEventHandler: handler,
    });

    const queryHandlerProps: DatabaseQueryHandlerProps & HandlerProps = {
      handler: props.handler,
      username: props.namespace.attrNamespaceAdminUsername,
      workGroupName: props.workGroup.workgroupName,
      databaseName: props.namespace.attrNamespaceDbName,
      ...props.properties,
    };
    this.resource = new cdk.CustomResource(this, 'Resource', {
      resourceType: 'Custom::RedshiftDatabaseQuery',
      serviceToken: provider.serviceToken,
      removalPolicy: props.removalPolicy,
      properties: queryHandlerProps,
    });

    this.grantPrincipal = handler.grantPrincipal;
    this.ref = this.resource.ref;
  }

  public applyRemovalPolicy(policy: cdk.RemovalPolicy): void {
    this.resource.applyRemovalPolicy(policy);
  }

  public getAtt(attributeName: string): cdk.Reference {
    return this.resource.getAtt(attributeName);
  }

  public getAttString(attributeName: string): string {
    return this.resource.getAttString(attributeName);
  }

  private getAdminUser(props: DatabaseOptions): string {
    return props.namespace.attrNamespaceAdminUsername;
  }
}