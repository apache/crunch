/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.crunch.scrunch

import org.apache.crunch.TupleN
import org.apache.crunch.types.PType

trait GeneratedTuplePTypeFamily extends BasePTypeFamily {
  import GeneratedTupleHelper._

  def tuple5[A, B, C, D, E](p0: PType[A], p1: PType[B], p2: PType[C], p3: PType[D], p4: PType[E]) = {
    val in = (t: TupleN) => (t.get(0).asInstanceOf[A],t.get(1).asInstanceOf[B],t.get(2).asInstanceOf[C],t.get(3).asInstanceOf[D],t.get(4).asInstanceOf[E])
    val out = (t: (A, B, C, D, E)) => tupleN(t._1, t._2, t._3, t._4, t._5)
    derived(classOf[(A, B, C, D, E)], in, out, ptf.tuples(p0, p1, p2, p3, p4))
  }

  def tuple6[A, B, C, D, E, F](p0: PType[A], p1: PType[B], p2: PType[C], p3: PType[D], p4: PType[E], p5: PType[F]) = {
    val in = (t: TupleN) => (t.get(0).asInstanceOf[A],t.get(1).asInstanceOf[B],t.get(2).asInstanceOf[C],t.get(3).asInstanceOf[D],t.get(4).asInstanceOf[E],t.get(5).asInstanceOf[F])
    val out = (t: (A, B, C, D, E, F)) => tupleN(t._1, t._2, t._3, t._4, t._5, t._6)
    derived(classOf[(A, B, C, D, E, F)], in, out, ptf.tuples(p0, p1, p2, p3, p4, p5))
  }

  def tuple7[A, B, C, D, E, F, G](p0: PType[A], p1: PType[B], p2: PType[C], p3: PType[D], p4: PType[E], p5: PType[F], p6: PType[G]) = {
    val in = (t: TupleN) => (t.get(0).asInstanceOf[A],t.get(1).asInstanceOf[B],t.get(2).asInstanceOf[C],t.get(3).asInstanceOf[D],t.get(4).asInstanceOf[E],t.get(5).asInstanceOf[F],t.get(6).asInstanceOf[G])
    val out = (t: (A, B, C, D, E, F, G)) => tupleN(t._1, t._2, t._3, t._4, t._5, t._6, t._7)
    derived(classOf[(A, B, C, D, E, F, G)], in, out, ptf.tuples(p0, p1, p2, p3, p4, p5, p6))
  }

  def tuple8[A, B, C, D, E, F, G, H](p0: PType[A], p1: PType[B], p2: PType[C], p3: PType[D], p4: PType[E], p5: PType[F], p6: PType[G], p7: PType[H]) = {
    val in = (t: TupleN) => (t.get(0).asInstanceOf[A],t.get(1).asInstanceOf[B],t.get(2).asInstanceOf[C],t.get(3).asInstanceOf[D],t.get(4).asInstanceOf[E],t.get(5).asInstanceOf[F],t.get(6).asInstanceOf[G],t.get(7).asInstanceOf[H])
    val out = (t: (A, B, C, D, E, F, G, H)) => tupleN(t._1, t._2, t._3, t._4, t._5, t._6, t._7, t._8)
    derived(classOf[(A, B, C, D, E, F, G, H)], in, out, ptf.tuples(p0, p1, p2, p3, p4, p5, p6, p7))
  }

  def tuple9[A, B, C, D, E, F, G, H, I](p0: PType[A], p1: PType[B], p2: PType[C], p3: PType[D], p4: PType[E], p5: PType[F], p6: PType[G], p7: PType[H], p8: PType[I]) = {
    val in = (t: TupleN) => (t.get(0).asInstanceOf[A],t.get(1).asInstanceOf[B],t.get(2).asInstanceOf[C],t.get(3).asInstanceOf[D],t.get(4).asInstanceOf[E],t.get(5).asInstanceOf[F],t.get(6).asInstanceOf[G],t.get(7).asInstanceOf[H],t.get(8).asInstanceOf[I])
    val out = (t: (A, B, C, D, E, F, G, H, I)) => tupleN(t._1, t._2, t._3, t._4, t._5, t._6, t._7, t._8, t._9)
    derived(classOf[(A, B, C, D, E, F, G, H, I)], in, out, ptf.tuples(p0, p1, p2, p3, p4, p5, p6, p7, p8))
  }

  def tuple10[A, B, C, D, E, F, G, H, I, J](p0: PType[A], p1: PType[B], p2: PType[C], p3: PType[D], p4: PType[E], p5: PType[F], p6: PType[G], p7: PType[H], p8: PType[I], p9: PType[J]) = {
    val in = (t: TupleN) => (t.get(0).asInstanceOf[A],t.get(1).asInstanceOf[B],t.get(2).asInstanceOf[C],t.get(3).asInstanceOf[D],t.get(4).asInstanceOf[E],t.get(5).asInstanceOf[F],t.get(6).asInstanceOf[G],t.get(7).asInstanceOf[H],t.get(8).asInstanceOf[I],t.get(9).asInstanceOf[J])
    val out = (t: (A, B, C, D, E, F, G, H, I, J)) => tupleN(t._1, t._2, t._3, t._4, t._5, t._6, t._7, t._8, t._9, t._10)
    derived(classOf[(A, B, C, D, E, F, G, H, I, J)], in, out, ptf.tuples(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9))
  }

  def tuple11[A, B, C, D, E, F, G, H, I, J, K](p0: PType[A], p1: PType[B], p2: PType[C], p3: PType[D], p4: PType[E], p5: PType[F], p6: PType[G], p7: PType[H], p8: PType[I], p9: PType[J], p10: PType[K]) = {
    val in = (t: TupleN) => (t.get(0).asInstanceOf[A],t.get(1).asInstanceOf[B],t.get(2).asInstanceOf[C],t.get(3).asInstanceOf[D],t.get(4).asInstanceOf[E],t.get(5).asInstanceOf[F],t.get(6).asInstanceOf[G],t.get(7).asInstanceOf[H],t.get(8).asInstanceOf[I],t.get(9).asInstanceOf[J],t.get(10).asInstanceOf[K])
    val out = (t: (A, B, C, D, E, F, G, H, I, J, K)) => tupleN(t._1, t._2, t._3, t._4, t._5, t._6, t._7, t._8, t._9, t._10, t._11)
    derived(classOf[(A, B, C, D, E, F, G, H, I, J, K)], in, out, ptf.tuples(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10))
  }

  def tuple12[A, B, C, D, E, F, G, H, I, J, K, L](p0: PType[A], p1: PType[B], p2: PType[C], p3: PType[D], p4: PType[E], p5: PType[F], p6: PType[G], p7: PType[H], p8: PType[I], p9: PType[J], p10: PType[K], p11: PType[L]) = {
    val in = (t: TupleN) => (t.get(0).asInstanceOf[A],t.get(1).asInstanceOf[B],t.get(2).asInstanceOf[C],t.get(3).asInstanceOf[D],t.get(4).asInstanceOf[E],t.get(5).asInstanceOf[F],t.get(6).asInstanceOf[G],t.get(7).asInstanceOf[H],t.get(8).asInstanceOf[I],t.get(9).asInstanceOf[J],t.get(10).asInstanceOf[K],t.get(11).asInstanceOf[L])
    val out = (t: (A, B, C, D, E, F, G, H, I, J, K, L)) => tupleN(t._1, t._2, t._3, t._4, t._5, t._6, t._7, t._8, t._9, t._10, t._11, t._12)
    derived(classOf[(A, B, C, D, E, F, G, H, I, J, K, L)], in, out, ptf.tuples(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11))
  }

  def tuple13[A, B, C, D, E, F, G, H, I, J, K, L, M](p0: PType[A], p1: PType[B], p2: PType[C], p3: PType[D], p4: PType[E], p5: PType[F], p6: PType[G], p7: PType[H], p8: PType[I], p9: PType[J], p10: PType[K], p11: PType[L], p12: PType[M]) = {
    val in = (t: TupleN) => (t.get(0).asInstanceOf[A],t.get(1).asInstanceOf[B],t.get(2).asInstanceOf[C],t.get(3).asInstanceOf[D],t.get(4).asInstanceOf[E],t.get(5).asInstanceOf[F],t.get(6).asInstanceOf[G],t.get(7).asInstanceOf[H],t.get(8).asInstanceOf[I],t.get(9).asInstanceOf[J],t.get(10).asInstanceOf[K],t.get(11).asInstanceOf[L],t.get(12).asInstanceOf[M])
    val out = (t: (A, B, C, D, E, F, G, H, I, J, K, L, M)) => tupleN(t._1, t._2, t._3, t._4, t._5, t._6, t._7, t._8, t._9, t._10, t._11, t._12, t._13)
    derived(classOf[(A, B, C, D, E, F, G, H, I, J, K, L, M)], in, out, ptf.tuples(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12))
  }

  def tuple14[A, B, C, D, E, F, G, H, I, J, K, L, M, N](p0: PType[A], p1: PType[B], p2: PType[C], p3: PType[D], p4: PType[E], p5: PType[F], p6: PType[G], p7: PType[H], p8: PType[I], p9: PType[J], p10: PType[K], p11: PType[L], p12: PType[M], p13: PType[N]) = {
    val in = (t: TupleN) => (t.get(0).asInstanceOf[A],t.get(1).asInstanceOf[B],t.get(2).asInstanceOf[C],t.get(3).asInstanceOf[D],t.get(4).asInstanceOf[E],t.get(5).asInstanceOf[F],t.get(6).asInstanceOf[G],t.get(7).asInstanceOf[H],t.get(8).asInstanceOf[I],t.get(9).asInstanceOf[J],t.get(10).asInstanceOf[K],t.get(11).asInstanceOf[L],t.get(12).asInstanceOf[M],t.get(13).asInstanceOf[N])
    val out = (t: (A, B, C, D, E, F, G, H, I, J, K, L, M, N)) => tupleN(t._1, t._2, t._3, t._4, t._5, t._6, t._7, t._8, t._9, t._10, t._11, t._12, t._13, t._14)
    derived(classOf[(A, B, C, D, E, F, G, H, I, J, K, L, M, N)], in, out, ptf.tuples(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13))
  }

  def tuple15[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O](p0: PType[A], p1: PType[B], p2: PType[C], p3: PType[D], p4: PType[E], p5: PType[F], p6: PType[G], p7: PType[H], p8: PType[I], p9: PType[J], p10: PType[K], p11: PType[L], p12: PType[M], p13: PType[N], p14: PType[O]) = {
    val in = (t: TupleN) => (t.get(0).asInstanceOf[A],t.get(1).asInstanceOf[B],t.get(2).asInstanceOf[C],t.get(3).asInstanceOf[D],t.get(4).asInstanceOf[E],t.get(5).asInstanceOf[F],t.get(6).asInstanceOf[G],t.get(7).asInstanceOf[H],t.get(8).asInstanceOf[I],t.get(9).asInstanceOf[J],t.get(10).asInstanceOf[K],t.get(11).asInstanceOf[L],t.get(12).asInstanceOf[M],t.get(13).asInstanceOf[N],t.get(14).asInstanceOf[O])
    val out = (t: (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O)) => tupleN(t._1, t._2, t._3, t._4, t._5, t._6, t._7, t._8, t._9, t._10, t._11, t._12, t._13, t._14, t._15)
    derived(classOf[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O)], in, out, ptf.tuples(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14))
  }

  def tuple16[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P](p0: PType[A], p1: PType[B], p2: PType[C], p3: PType[D], p4: PType[E], p5: PType[F], p6: PType[G], p7: PType[H], p8: PType[I], p9: PType[J], p10: PType[K], p11: PType[L], p12: PType[M], p13: PType[N], p14: PType[O], p15: PType[P]) = {
    val in = (t: TupleN) => (t.get(0).asInstanceOf[A],t.get(1).asInstanceOf[B],t.get(2).asInstanceOf[C],t.get(3).asInstanceOf[D],t.get(4).asInstanceOf[E],t.get(5).asInstanceOf[F],t.get(6).asInstanceOf[G],t.get(7).asInstanceOf[H],t.get(8).asInstanceOf[I],t.get(9).asInstanceOf[J],t.get(10).asInstanceOf[K],t.get(11).asInstanceOf[L],t.get(12).asInstanceOf[M],t.get(13).asInstanceOf[N],t.get(14).asInstanceOf[O],t.get(15).asInstanceOf[P])
    val out = (t: (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P)) => tupleN(t._1, t._2, t._3, t._4, t._5, t._6, t._7, t._8, t._9, t._10, t._11, t._12, t._13, t._14, t._15, t._16)
    derived(classOf[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P)], in, out, ptf.tuples(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15))
  }

  def tuple17[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q](p0: PType[A], p1: PType[B], p2: PType[C], p3: PType[D], p4: PType[E], p5: PType[F], p6: PType[G], p7: PType[H], p8: PType[I], p9: PType[J], p10: PType[K], p11: PType[L], p12: PType[M], p13: PType[N], p14: PType[O], p15: PType[P], p16: PType[Q]) = {
    val in = (t: TupleN) => (t.get(0).asInstanceOf[A],t.get(1).asInstanceOf[B],t.get(2).asInstanceOf[C],t.get(3).asInstanceOf[D],t.get(4).asInstanceOf[E],t.get(5).asInstanceOf[F],t.get(6).asInstanceOf[G],t.get(7).asInstanceOf[H],t.get(8).asInstanceOf[I],t.get(9).asInstanceOf[J],t.get(10).asInstanceOf[K],t.get(11).asInstanceOf[L],t.get(12).asInstanceOf[M],t.get(13).asInstanceOf[N],t.get(14).asInstanceOf[O],t.get(15).asInstanceOf[P],t.get(16).asInstanceOf[Q])
    val out = (t: (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q)) => tupleN(t._1, t._2, t._3, t._4, t._5, t._6, t._7, t._8, t._9, t._10, t._11, t._12, t._13, t._14, t._15, t._16, t._17)
    derived(classOf[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q)], in, out, ptf.tuples(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16))
  }

  def tuple18[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R](p0: PType[A], p1: PType[B], p2: PType[C], p3: PType[D], p4: PType[E], p5: PType[F], p6: PType[G], p7: PType[H], p8: PType[I], p9: PType[J], p10: PType[K], p11: PType[L], p12: PType[M], p13: PType[N], p14: PType[O], p15: PType[P], p16: PType[Q], p17: PType[R]) = {
    val in = (t: TupleN) => (t.get(0).asInstanceOf[A],t.get(1).asInstanceOf[B],t.get(2).asInstanceOf[C],t.get(3).asInstanceOf[D],t.get(4).asInstanceOf[E],t.get(5).asInstanceOf[F],t.get(6).asInstanceOf[G],t.get(7).asInstanceOf[H],t.get(8).asInstanceOf[I],t.get(9).asInstanceOf[J],t.get(10).asInstanceOf[K],t.get(11).asInstanceOf[L],t.get(12).asInstanceOf[M],t.get(13).asInstanceOf[N],t.get(14).asInstanceOf[O],t.get(15).asInstanceOf[P],t.get(16).asInstanceOf[Q],t.get(17).asInstanceOf[R])
    val out = (t: (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R)) => tupleN(t._1, t._2, t._3, t._4, t._5, t._6, t._7, t._8, t._9, t._10, t._11, t._12, t._13, t._14, t._15, t._16, t._17, t._18)
    derived(classOf[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R)], in, out, ptf.tuples(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17))
  }

  def tuple19[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S](p0: PType[A], p1: PType[B], p2: PType[C], p3: PType[D], p4: PType[E], p5: PType[F], p6: PType[G], p7: PType[H], p8: PType[I], p9: PType[J], p10: PType[K], p11: PType[L], p12: PType[M], p13: PType[N], p14: PType[O], p15: PType[P], p16: PType[Q], p17: PType[R], p18: PType[S]) = {
    val in = (t: TupleN) => (t.get(0).asInstanceOf[A],t.get(1).asInstanceOf[B],t.get(2).asInstanceOf[C],t.get(3).asInstanceOf[D],t.get(4).asInstanceOf[E],t.get(5).asInstanceOf[F],t.get(6).asInstanceOf[G],t.get(7).asInstanceOf[H],t.get(8).asInstanceOf[I],t.get(9).asInstanceOf[J],t.get(10).asInstanceOf[K],t.get(11).asInstanceOf[L],t.get(12).asInstanceOf[M],t.get(13).asInstanceOf[N],t.get(14).asInstanceOf[O],t.get(15).asInstanceOf[P],t.get(16).asInstanceOf[Q],t.get(17).asInstanceOf[R],t.get(18).asInstanceOf[S])
    val out = (t: (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S)) => tupleN(t._1, t._2, t._3, t._4, t._5, t._6, t._7, t._8, t._9, t._10, t._11, t._12, t._13, t._14, t._15, t._16, t._17, t._18, t._19)
    derived(classOf[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S)], in, out, ptf.tuples(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18))
  }

  def tuple20[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T](p0: PType[A], p1: PType[B], p2: PType[C], p3: PType[D], p4: PType[E], p5: PType[F], p6: PType[G], p7: PType[H], p8: PType[I], p9: PType[J], p10: PType[K], p11: PType[L], p12: PType[M], p13: PType[N], p14: PType[O], p15: PType[P], p16: PType[Q], p17: PType[R], p18: PType[S], p19: PType[T]) = {
    val in = (t: TupleN) => (t.get(0).asInstanceOf[A],t.get(1).asInstanceOf[B],t.get(2).asInstanceOf[C],t.get(3).asInstanceOf[D],t.get(4).asInstanceOf[E],t.get(5).asInstanceOf[F],t.get(6).asInstanceOf[G],t.get(7).asInstanceOf[H],t.get(8).asInstanceOf[I],t.get(9).asInstanceOf[J],t.get(10).asInstanceOf[K],t.get(11).asInstanceOf[L],t.get(12).asInstanceOf[M],t.get(13).asInstanceOf[N],t.get(14).asInstanceOf[O],t.get(15).asInstanceOf[P],t.get(16).asInstanceOf[Q],t.get(17).asInstanceOf[R],t.get(18).asInstanceOf[S],t.get(19).asInstanceOf[T])
    val out = (t: (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T)) => tupleN(t._1, t._2, t._3, t._4, t._5, t._6, t._7, t._8, t._9, t._10, t._11, t._12, t._13, t._14, t._15, t._16, t._17, t._18, t._19, t._20)
    derived(classOf[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T)], in, out, ptf.tuples(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18, p19))
  }

  def tuple21[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U](p0: PType[A], p1: PType[B], p2: PType[C], p3: PType[D], p4: PType[E], p5: PType[F], p6: PType[G], p7: PType[H], p8: PType[I], p9: PType[J], p10: PType[K], p11: PType[L], p12: PType[M], p13: PType[N], p14: PType[O], p15: PType[P], p16: PType[Q], p17: PType[R], p18: PType[S], p19: PType[T], p20: PType[U]) = {
    val in = (t: TupleN) => (t.get(0).asInstanceOf[A],t.get(1).asInstanceOf[B],t.get(2).asInstanceOf[C],t.get(3).asInstanceOf[D],t.get(4).asInstanceOf[E],t.get(5).asInstanceOf[F],t.get(6).asInstanceOf[G],t.get(7).asInstanceOf[H],t.get(8).asInstanceOf[I],t.get(9).asInstanceOf[J],t.get(10).asInstanceOf[K],t.get(11).asInstanceOf[L],t.get(12).asInstanceOf[M],t.get(13).asInstanceOf[N],t.get(14).asInstanceOf[O],t.get(15).asInstanceOf[P],t.get(16).asInstanceOf[Q],t.get(17).asInstanceOf[R],t.get(18).asInstanceOf[S],t.get(19).asInstanceOf[T],t.get(20).asInstanceOf[U])
    val out = (t: (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U)) => tupleN(t._1, t._2, t._3, t._4, t._5, t._6, t._7, t._8, t._9, t._10, t._11, t._12, t._13, t._14, t._15, t._16, t._17, t._18, t._19, t._20, t._21)
    derived(classOf[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U)], in, out, ptf.tuples(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18, p19, p20))
  }

  def tuple22[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V](p0: PType[A], p1: PType[B], p2: PType[C], p3: PType[D], p4: PType[E], p5: PType[F], p6: PType[G], p7: PType[H], p8: PType[I], p9: PType[J], p10: PType[K], p11: PType[L], p12: PType[M], p13: PType[N], p14: PType[O], p15: PType[P], p16: PType[Q], p17: PType[R], p18: PType[S], p19: PType[T], p20: PType[U], p21: PType[V]) = {
    val in = (t: TupleN) => (t.get(0).asInstanceOf[A],t.get(1).asInstanceOf[B],t.get(2).asInstanceOf[C],t.get(3).asInstanceOf[D],t.get(4).asInstanceOf[E],t.get(5).asInstanceOf[F],t.get(6).asInstanceOf[G],t.get(7).asInstanceOf[H],t.get(8).asInstanceOf[I],t.get(9).asInstanceOf[J],t.get(10).asInstanceOf[K],t.get(11).asInstanceOf[L],t.get(12).asInstanceOf[M],t.get(13).asInstanceOf[N],t.get(14).asInstanceOf[O],t.get(15).asInstanceOf[P],t.get(16).asInstanceOf[Q],t.get(17).asInstanceOf[R],t.get(18).asInstanceOf[S],t.get(19).asInstanceOf[T],t.get(20).asInstanceOf[U],t.get(21).asInstanceOf[V])
    val out = (t: (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V)) => tupleN(t._1, t._2, t._3, t._4, t._5, t._6, t._7, t._8, t._9, t._10, t._11, t._12, t._13, t._14, t._15, t._16, t._17, t._18, t._19, t._20, t._21, t._22)
    derived(classOf[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V)], in, out, ptf.tuples(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18, p19, p20, p21))
  }

}

