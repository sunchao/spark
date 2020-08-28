/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.Literal.FalseLiteral
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types._

/**
 * Unwrap casts in binary comparison operations with patterns like following:
 *
 * `BinaryComparison(Cast(fromExp, toType), Literal(value, toType))`
 *
 * This rule optimizes expressions with the above pattern by either replacing the cast with simpler
 * constructs, or moving the cast from the expression side to the literal side, which enables them
 * to be optimized away later and pushed down to data sources.
 *
 * Currently this only handles the case where `fromType` (of `fromExp`) and `toType` are of
 * integral types (i.e., byte, short, int and long). The rule checks to see if the literal `value`
 * is within range `(min, max)`, where `min` and `max` are the minimum and maximum value of
 * `fromType`. If this is true then it means we can safely cast `value` to `fromType` and thus able
 * to move the cast to the literal side.
 *
 * If the `value` is not within range `(min, max)`, the rule breaks the scenario into different
 * cases and try to replace each case with simpler constructs.
 *
 * if `value > max`, the cases are of following:
 *  - `cast(exp, ty) > value` ==> if(isnull(exp), null, false)
 *  - `cast(exp, ty) >= value` ==> if(isnull(exp), null, false)
 *  - `cast(exp, ty) === value` ==> if(isnull(exp), null, false)
 *  - `cast(exp, ty) <=> value` ==> false
 *  - `cast(exp, ty) <= value` ==> if(isnull(exp), null, true)
 *  - `cast(exp, ty) < value` ==> if(isnull(exp), null, true)
 *
 * if `value == max`, the cases are of following:
 *  - `cast(exp, ty) > value` ==> if(isnull(exp), null, false)
 *  - `cast(exp, ty) >= value` ==> exp == max
 *  - `cast(exp, ty) === value` ==> exp == max
 *  - `cast(exp, ty) <=> value` ==> exp == max
 *  - `cast(exp, ty) <= value` ==> if(isnull(exp), null, true)
 *  - `cast(exp, ty) < value` ==> exp =!= max
 *
 * Similarly for the cases when `value == min` and `value < min`.
 */
object UnwrapCast extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case l: LogicalPlan => l transformExpressionsUp {
      case e @ BinaryComparison(_, _) => unwrapCast(e)
    }
  }

  private def unwrapCast(exp: Expression): Expression = exp match {
    case BinaryComparison(Literal(_, _), Cast(_, _, _)) =>
      def swap(e: Expression): Expression = e match {
        case GreaterThan(left, right) => LessThan(right, left)
        case GreaterThanOrEqual(left, right) => LessThanOrEqual(right, left)
        case EqualTo(left, right) => EqualTo(right, left)
        case EqualNullSafe(left, right) => EqualNullSafe(right, left)
        case LessThanOrEqual(left, right) => GreaterThanOrEqual(right, left)
        case LessThan(left, right) => GreaterThan(right, left)
        case _ => e
      }

      swap(unwrapCast(swap(exp)))

    case BinaryComparison(Cast(fromExp, _, _), Literal(value, toType))
      if canImplicitlyCast(fromExp, toType) =>

      // Check if the literal value is within the range of the `fromType`, and handle the boundary
      // cases in the following
      val fromType = fromExp.dataType

      val result = handleIntegralTypeBoundary(exp, fromExp, fromType.asInstanceOf[IntegralType],
        toType.asInstanceOf[IntegralType], value)
      if (result.isDefined) {
        return result.get
      }

      // Now we can assume `value` is within the bound of the source type, e.g., min < value < max
      val lit = Cast(Literal(value), fromType)
      exp match {
        case GreaterThan(_, _) => GreaterThan(fromExp, lit)
        case GreaterThanOrEqual(_, _) => GreaterThanOrEqual(fromExp, lit)
        case EqualTo(_, _) => EqualTo(fromExp, lit)
        case EqualNullSafe(_, _) => EqualNullSafe(fromExp, lit)
        case LessThan(_, _) => LessThan(fromExp, lit)
        case LessThanOrEqual(_, _) => LessThanOrEqual(fromExp, lit)
      }

    case _ => exp
  }

  /**
   * Check if the input `value` is within range `(min, max)` of the `fromType`, where `min` and
   * `max` are the minimum and maximum value of the `fromType`. If the above is not true, this
   * replaces the input binary comparison `exp` with simpler expressions.
   *
   * Returns `None` when the input value is within range `(min, max)`. Otherwise, returns the
   * optimized expression wrapped in `Some`.
   */
  private def handleIntegralTypeBoundary(
      exp: Expression,
      fromExp: Expression,
      fromType: IntegralType,
      toType: IntegralType,
      value: Any): Option[Expression] = {

    val (min, max) = getRange(fromType)
    val (minInToType, maxInToType) =
      (Cast(Literal(min), toType).eval(), Cast(Literal(max), toType).eval())

    // Compare upper bounds
    val maxCmp = toType.ordering.asInstanceOf[Ordering[Any]].compare(value, maxInToType)
    val minCmp = toType.ordering.asInstanceOf[Ordering[Any]].compare(value, minInToType)

    if (maxCmp > 0) {
      Some(exp match {
        case EqualTo(_, _) | GreaterThan(_, _) | GreaterThanOrEqual(_, _) =>
          falseIfNotNull(fromExp)
        case LessThan(_, _) | LessThanOrEqual(_, _) =>
          trueIfNotNull(fromExp)
        case EqualNullSafe(_, _) =>
          FalseLiteral
        case _ => exp // impossible but safe guard, same below
      })
    } else if (maxCmp == 0) {
      Some(exp match {
        case GreaterThan(_, _) =>
          falseIfNotNull(fromExp)
        case LessThanOrEqual(_, _) =>
          trueIfNotNull(fromExp)
        case LessThan(_, _) =>
          Not(EqualTo(fromExp, Literal(max, fromType)))
        case GreaterThanOrEqual(_, _) | EqualTo(_, _) | EqualNullSafe(_, _) =>
          EqualTo(fromExp, Literal(max, fromType))
        case _ => exp
      })
    } else if (minCmp < 0) {
      Some(exp match {
        case GreaterThan(_, _) | GreaterThanOrEqual(_, _) =>
          trueIfNotNull(fromExp)
        case LessThan(_, _) | LessThanOrEqual(_, _) | EqualTo(_, _) =>
          falseIfNotNull(fromExp)
        case EqualNullSafe(_, _) =>
          FalseLiteral
        case _ => exp
      })
    } else if (minCmp == 0) {
      Some(exp match {
        case LessThan(_, _) =>
          falseIfNotNull(fromExp)
        case GreaterThanOrEqual(_, _) =>
          trueIfNotNull(fromExp)
        case GreaterThan(_, _) =>
          Not(EqualTo(fromExp, Literal(min, fromType)))
        case LessThanOrEqual(_, _) | EqualTo(_, _) | EqualNullSafe(_, _) =>
          EqualTo(fromExp, Literal(min, fromType))
        case _ => exp
      })
    } else {
      // minCmp > 0 && maxCmp < 0
      None
    }
  }

  private def canImplicitlyCast(fromExp: Expression, toType: DataType): Boolean = {
    fromExp.dataType.isInstanceOf[IntegralType] && toType.isInstanceOf[IntegralType] &&
      Cast.canUpCast(fromExp.dataType, toType)
  }

  /**
   * Wraps input expression `e` with `if(isnull(e), null, false)`. The if-clause is represented
   * using `and(isnull(e), null)` which is semantically equivalent.
   */
  private[sql] def falseIfNotNull(e: Expression): Expression =
    And(IsNull(e), Literal(null, BooleanType))

  /**
   * Wraps input expression `e` with `if(isnull(e), null, true)`. The if-clause is represented
   * using `or(isnotnull(e), null)` which is semantically equivalent.
   */
  private[sql] def trueIfNotNull(e: Expression): Expression =
    Or(IsNotNull(e), Literal(null, BooleanType))

  private def getRange(ty: DataType): (Any, Any) = ty match {
    case ByteType => (Byte.MinValue, Byte.MaxValue)
    case ShortType => (Short.MinValue, Short.MaxValue)
    case IntegerType => (Int.MinValue, Int.MaxValue)
    case LongType => (Long.MinValue, Long.MaxValue)
  }
}


