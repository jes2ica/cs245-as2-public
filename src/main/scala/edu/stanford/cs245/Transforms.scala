package edu.stanford.cs245

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{And, Ascending, BinaryComparison, EqualTo, Expression, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual, Literal, Multiply, ScalaUDF, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Sort}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.{BooleanType, DoubleType}

object Transforms {

  // check if a ScalaUDF Expression is our dist UDF
  def isDistUdf(udf: ScalaUDF): Boolean = {
    udf.udfName.getOrElse("") == "dist"
  }

  // get an Expression representing the dist_sq UDF with the provided
  // arguments
  def getDistSqUdf(args: Seq[Expression]): ScalaUDF = {
    ScalaUDF(
      (x1: Double, y1: Double, x2: Double, y2: Double) => {
        val xDiff = x1 - x2;
        val yDiff = y1 - y2;
        xDiff * xDiff + yDiff * yDiff
      }, DoubleType, args, Seq(DoubleType, DoubleType, DoubleType, DoubleType),
      udfName = Option.apply("dist_sq"))
  }

  def getSq(val1: Double): Double = {
    val1 * val1
  }

  def updateOrder(o: SortOrder) : SortOrder = {
    if (o.child.isInstanceOf[ScalaUDF]) {
      SortOrder(getDistSqUdf(o.child.asInstanceOf[ScalaUDF].children), Ascending)
    } else {
      o
    }
  }

  // Return any additional optimization passes here
  def getOptimizationPasses(spark: SparkSession): Seq[Rule[LogicalPlan]] = {
    Seq(EliminateZeroDists(spark), SimplifyComparison(spark), EliminateSquareRoot(spark), SimplifyOrderOperation(spark))
  }

  case class EliminateZeroDists(spark: SparkSession) extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
      case udf: ScalaUDF if isDistUdf(udf) && udf.children(0) == udf.children(2) &&
        udf.children(1) == udf.children(3) => Literal(0.0, DoubleType)
    }
  }

  case class SimplifyComparison(spark: SparkSession) extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
      case LessThan(udf: ScalaUDF, right: Literal) if isDistUdf(udf) && right.value.asInstanceOf[Double] <= 0.0 =>
        Literal(false, BooleanType)
      case EqualTo(udf: ScalaUDF, right: Literal) if isDistUdf(udf) && right.value.asInstanceOf[Double] < 0.0 =>
        Literal(false, BooleanType)
      case EqualTo(udf: ScalaUDF, right: Literal) if isDistUdf(udf) && right.value.asInstanceOf[Double] >= 0  =>
        And(EqualTo(udf.children(0), udf.children(2)), EqualTo(udf.children(1), udf.children(3)))
      case GreaterThanOrEqual(left: Literal, udf: ScalaUDF) if isDistUdf(udf) && left.value.asInstanceOf[Double] < 0.0 =>
        Literal(false, BooleanType)
      case LessThanOrEqual(left: Literal, udf: ScalaUDF) if isDistUdf(udf) && left.value.asInstanceOf[Double] <= 0.0 =>
        Literal(true, BooleanType)
    }
  }

  case class EliminateSquareRoot(spark: SparkSession) extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
      case GreaterThan(udf: ScalaUDF, right: Literal) if isDistUdf(udf) && right.value.isInstanceOf[Double] =>
        GreaterThan(getDistSqUdf(udf.children), Literal(getSq(right.value.asInstanceOf[Double])))
      case GreaterThan(left: ScalaUDF, right: ScalaUDF) if isDistUdf(left) && isDistUdf(right) =>
        GreaterThan(getDistSqUdf(left.children), getDistSqUdf(right.children))
    }
  }

  case class SimplifyOrderOperation(sparkSession: SparkSession) extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transform {
      case Sort(order, global, child) =>
        val newOrders = order.map(o => updateOrder(o))
        Sort(newOrders, global, child)
    }
  }
}


