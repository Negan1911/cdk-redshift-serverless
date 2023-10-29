import cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import { Namespace } from './namespace';
import { CfnWorkgroup } from "aws-cdk-lib/aws-redshiftserverless";

/**
 * Properties for defining a `Workgroup`
 *
 * @struct
 * @stability external
 *
 * @link http://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-resource-redshiftserverless-workgroup.html
 */
export interface WorkgroupProps {
  /**
   * The name of the workgroup.
   *
   * @link http://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-resource-redshiftserverless-workgroup.html#cfn-redshiftserverless-workgroup-workgroupname
   */
  readonly workgroupName: string;
  /**
   * The base compute capacity of the workgroup in Redshift Processing Units (RPUs).
   *
   * @link http://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-resource-redshiftserverless-workgroup.html#cfn-redshiftserverless-workgroup-basecapacity
   */
  readonly baseCapacity?: number;
  /**
   * A list of parameters to set for finer control over a database. Available options are `datestyle` , `enable_user_activity_logging` , `query_group` , `search_path` , and `max_query_execution_time` .
   *
   * @link http://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-resource-redshiftserverless-workgroup.html#cfn-redshiftserverless-workgroup-configparameters
   */
  readonly configParameters?: Array<CfnWorkgroup.ConfigParameterProperty | cdk.IResolvable> | cdk.IResolvable;
  /**
   * The value that specifies whether to enable enhanced virtual private cloud (VPC) routing, which forces Amazon Redshift Serverless to route traffic through your VPC.
   *
   * @link http://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-resource-redshiftserverless-workgroup.html#cfn-redshiftserverless-workgroup-enhancedvpcrouting
   */
  readonly enhancedVpcRouting?: boolean | cdk.IResolvable;
  /**
   * The namespace the workgroup is associated with.
   *
   * @link http://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-resource-redshiftserverless-workgroup.html#cfn-redshiftserverless-workgroup-namespacename
   */
  readonly namespace?: Namespace;
  /**
   * The custom port to use when connecting to a workgroup. Valid port ranges are 5431-5455 and 8191-8215. The default is 5439.
   *
   * @link http://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-resource-redshiftserverless-workgroup.html#cfn-redshiftserverless-workgroup-port
   */
  readonly port?: number;
  /**
   * A value that specifies whether the workgroup can be accessible from a public network.
   *
   * @link http://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-resource-redshiftserverless-workgroup.html#cfn-redshiftserverless-workgroup-publiclyaccessible
   */
  readonly publiclyAccessible?: boolean | cdk.IResolvable;
  /**
   * A list of security group IDs to associate with the workgroup.
   *
   * @link http://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-resource-redshiftserverless-workgroup.html#cfn-redshiftserverless-workgroup-securitygroupids
   */
  readonly securityGroupIds?: string[];
  /**
   * A list of subnet IDs the workgroup is associated with.
   *
   * @link http://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-resource-redshiftserverless-workgroup.html#cfn-redshiftserverless-workgroup-subnetids
   */
  readonly subnetIds?: string[];
  /**
   * The map of the key-value pairs used to tag the workgroup.
   *
   * @link http://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-resource-redshiftserverless-workgroup.html#cfn-redshiftserverless-workgroup-tags
   */
  readonly tags?: cdk.CfnTag[];
}

export class Workgroup extends CfnWorkgroup {
  constructor(scope: Construct, id: string, props: WorkgroupProps) {
    super(scope, id, {
      ...props,
      namespaceName: props.namespace?.namespaceName
    })

    if (props.namespace)
      this.addDependency(props.namespace.cfnNamespace)
  }
}