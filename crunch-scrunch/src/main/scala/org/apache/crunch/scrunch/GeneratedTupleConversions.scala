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

trait GeneratedTupleConversions {
  implicit def tuple5[A: PTypeH, B: PTypeH, C: PTypeH, D: PTypeH, E: PTypeH] = new PTypeH[(A, B, C, D, E)] {
    def get(ptf: PTypeFamily) = {
      ptf.tuple5(implicitly[PTypeH[A]].get(ptf), implicitly[PTypeH[B]].get(ptf), implicitly[PTypeH[C]].get(ptf), implicitly[PTypeH[D]].get(ptf), implicitly[PTypeH[E]].get(ptf))
    }
  }

  implicit def tuple6[A: PTypeH, B: PTypeH, C: PTypeH, D: PTypeH, E: PTypeH, F: PTypeH] = new PTypeH[(A, B, C, D, E, F)] {
    def get(ptf: PTypeFamily) = {
      ptf.tuple6(implicitly[PTypeH[A]].get(ptf), implicitly[PTypeH[B]].get(ptf), implicitly[PTypeH[C]].get(ptf), implicitly[PTypeH[D]].get(ptf), implicitly[PTypeH[E]].get(ptf), implicitly[PTypeH[F]].get(ptf))
    }
  }

  implicit def tuple7[A: PTypeH, B: PTypeH, C: PTypeH, D: PTypeH, E: PTypeH, F: PTypeH, G: PTypeH] = new PTypeH[(A, B, C, D, E, F, G)] {
    def get(ptf: PTypeFamily) = {
      ptf.tuple7(implicitly[PTypeH[A]].get(ptf), implicitly[PTypeH[B]].get(ptf), implicitly[PTypeH[C]].get(ptf), implicitly[PTypeH[D]].get(ptf), implicitly[PTypeH[E]].get(ptf), implicitly[PTypeH[F]].get(ptf), implicitly[PTypeH[G]].get(ptf))
    }
  }

  implicit def tuple8[A: PTypeH, B: PTypeH, C: PTypeH, D: PTypeH, E: PTypeH, F: PTypeH, G: PTypeH, H: PTypeH] = new PTypeH[(A, B, C, D, E, F, G, H)] {
    def get(ptf: PTypeFamily) = {
      ptf.tuple8(implicitly[PTypeH[A]].get(ptf), implicitly[PTypeH[B]].get(ptf), implicitly[PTypeH[C]].get(ptf), implicitly[PTypeH[D]].get(ptf), implicitly[PTypeH[E]].get(ptf), implicitly[PTypeH[F]].get(ptf), implicitly[PTypeH[G]].get(ptf), implicitly[PTypeH[H]].get(ptf))
    }
  }

  implicit def tuple9[A: PTypeH, B: PTypeH, C: PTypeH, D: PTypeH, E: PTypeH, F: PTypeH, G: PTypeH, H: PTypeH, I: PTypeH] = new PTypeH[(A, B, C, D, E, F, G, H, I)] {
    def get(ptf: PTypeFamily) = {
      ptf.tuple9(implicitly[PTypeH[A]].get(ptf), implicitly[PTypeH[B]].get(ptf), implicitly[PTypeH[C]].get(ptf), implicitly[PTypeH[D]].get(ptf), implicitly[PTypeH[E]].get(ptf), implicitly[PTypeH[F]].get(ptf), implicitly[PTypeH[G]].get(ptf), implicitly[PTypeH[H]].get(ptf), implicitly[PTypeH[I]].get(ptf))
    }
  }

  implicit def tuple10[A: PTypeH, B: PTypeH, C: PTypeH, D: PTypeH, E: PTypeH, F: PTypeH, G: PTypeH, H: PTypeH, I: PTypeH, J: PTypeH] = new PTypeH[(A, B, C, D, E, F, G, H, I, J)] {
    def get(ptf: PTypeFamily) = {
      ptf.tuple10(implicitly[PTypeH[A]].get(ptf), implicitly[PTypeH[B]].get(ptf), implicitly[PTypeH[C]].get(ptf), implicitly[PTypeH[D]].get(ptf), implicitly[PTypeH[E]].get(ptf), implicitly[PTypeH[F]].get(ptf), implicitly[PTypeH[G]].get(ptf), implicitly[PTypeH[H]].get(ptf), implicitly[PTypeH[I]].get(ptf), implicitly[PTypeH[J]].get(ptf))
    }
  }

  implicit def tuple11[A: PTypeH, B: PTypeH, C: PTypeH, D: PTypeH, E: PTypeH, F: PTypeH, G: PTypeH, H: PTypeH, I: PTypeH, J: PTypeH, K: PTypeH] = new PTypeH[(A, B, C, D, E, F, G, H, I, J, K)] {
    def get(ptf: PTypeFamily) = {
      ptf.tuple11(implicitly[PTypeH[A]].get(ptf), implicitly[PTypeH[B]].get(ptf), implicitly[PTypeH[C]].get(ptf), implicitly[PTypeH[D]].get(ptf), implicitly[PTypeH[E]].get(ptf), implicitly[PTypeH[F]].get(ptf), implicitly[PTypeH[G]].get(ptf), implicitly[PTypeH[H]].get(ptf), implicitly[PTypeH[I]].get(ptf), implicitly[PTypeH[J]].get(ptf), implicitly[PTypeH[K]].get(ptf))
    }
  }

  implicit def tuple12[A: PTypeH, B: PTypeH, C: PTypeH, D: PTypeH, E: PTypeH, F: PTypeH, G: PTypeH, H: PTypeH, I: PTypeH, J: PTypeH, K: PTypeH, L: PTypeH] = new PTypeH[(A, B, C, D, E, F, G, H, I, J, K, L)] {
    def get(ptf: PTypeFamily) = {
      ptf.tuple12(implicitly[PTypeH[A]].get(ptf), implicitly[PTypeH[B]].get(ptf), implicitly[PTypeH[C]].get(ptf), implicitly[PTypeH[D]].get(ptf), implicitly[PTypeH[E]].get(ptf), implicitly[PTypeH[F]].get(ptf), implicitly[PTypeH[G]].get(ptf), implicitly[PTypeH[H]].get(ptf), implicitly[PTypeH[I]].get(ptf), implicitly[PTypeH[J]].get(ptf), implicitly[PTypeH[K]].get(ptf), implicitly[PTypeH[L]].get(ptf))
    }
  }

  implicit def tuple13[A: PTypeH, B: PTypeH, C: PTypeH, D: PTypeH, E: PTypeH, F: PTypeH, G: PTypeH, H: PTypeH, I: PTypeH, J: PTypeH, K: PTypeH, L: PTypeH, M: PTypeH] = new PTypeH[(A, B, C, D, E, F, G, H, I, J, K, L, M)] {
    def get(ptf: PTypeFamily) = {
      ptf.tuple13(implicitly[PTypeH[A]].get(ptf), implicitly[PTypeH[B]].get(ptf), implicitly[PTypeH[C]].get(ptf), implicitly[PTypeH[D]].get(ptf), implicitly[PTypeH[E]].get(ptf), implicitly[PTypeH[F]].get(ptf), implicitly[PTypeH[G]].get(ptf), implicitly[PTypeH[H]].get(ptf), implicitly[PTypeH[I]].get(ptf), implicitly[PTypeH[J]].get(ptf), implicitly[PTypeH[K]].get(ptf), implicitly[PTypeH[L]].get(ptf), implicitly[PTypeH[M]].get(ptf))
    }
  }

  implicit def tuple14[A: PTypeH, B: PTypeH, C: PTypeH, D: PTypeH, E: PTypeH, F: PTypeH, G: PTypeH, H: PTypeH, I: PTypeH, J: PTypeH, K: PTypeH, L: PTypeH, M: PTypeH, N: PTypeH] = new PTypeH[(A, B, C, D, E, F, G, H, I, J, K, L, M, N)] {
    def get(ptf: PTypeFamily) = {
      ptf.tuple14(implicitly[PTypeH[A]].get(ptf), implicitly[PTypeH[B]].get(ptf), implicitly[PTypeH[C]].get(ptf), implicitly[PTypeH[D]].get(ptf), implicitly[PTypeH[E]].get(ptf), implicitly[PTypeH[F]].get(ptf), implicitly[PTypeH[G]].get(ptf), implicitly[PTypeH[H]].get(ptf), implicitly[PTypeH[I]].get(ptf), implicitly[PTypeH[J]].get(ptf), implicitly[PTypeH[K]].get(ptf), implicitly[PTypeH[L]].get(ptf), implicitly[PTypeH[M]].get(ptf), implicitly[PTypeH[N]].get(ptf))
    }
  }

  implicit def tuple15[A: PTypeH, B: PTypeH, C: PTypeH, D: PTypeH, E: PTypeH, F: PTypeH, G: PTypeH, H: PTypeH, I: PTypeH, J: PTypeH, K: PTypeH, L: PTypeH, M: PTypeH, N: PTypeH, O: PTypeH] = new PTypeH[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O)] {
    def get(ptf: PTypeFamily) = {
      ptf.tuple15(implicitly[PTypeH[A]].get(ptf), implicitly[PTypeH[B]].get(ptf), implicitly[PTypeH[C]].get(ptf), implicitly[PTypeH[D]].get(ptf), implicitly[PTypeH[E]].get(ptf), implicitly[PTypeH[F]].get(ptf), implicitly[PTypeH[G]].get(ptf), implicitly[PTypeH[H]].get(ptf), implicitly[PTypeH[I]].get(ptf), implicitly[PTypeH[J]].get(ptf), implicitly[PTypeH[K]].get(ptf), implicitly[PTypeH[L]].get(ptf), implicitly[PTypeH[M]].get(ptf), implicitly[PTypeH[N]].get(ptf), implicitly[PTypeH[O]].get(ptf))
    }
  }

  implicit def tuple16[A: PTypeH, B: PTypeH, C: PTypeH, D: PTypeH, E: PTypeH, F: PTypeH, G: PTypeH, H: PTypeH, I: PTypeH, J: PTypeH, K: PTypeH, L: PTypeH, M: PTypeH, N: PTypeH, O: PTypeH, P: PTypeH] = new PTypeH[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P)] {
    def get(ptf: PTypeFamily) = {
      ptf.tuple16(implicitly[PTypeH[A]].get(ptf), implicitly[PTypeH[B]].get(ptf), implicitly[PTypeH[C]].get(ptf), implicitly[PTypeH[D]].get(ptf), implicitly[PTypeH[E]].get(ptf), implicitly[PTypeH[F]].get(ptf), implicitly[PTypeH[G]].get(ptf), implicitly[PTypeH[H]].get(ptf), implicitly[PTypeH[I]].get(ptf), implicitly[PTypeH[J]].get(ptf), implicitly[PTypeH[K]].get(ptf), implicitly[PTypeH[L]].get(ptf), implicitly[PTypeH[M]].get(ptf), implicitly[PTypeH[N]].get(ptf), implicitly[PTypeH[O]].get(ptf), implicitly[PTypeH[P]].get(ptf))
    }
  }

  implicit def tuple17[A: PTypeH, B: PTypeH, C: PTypeH, D: PTypeH, E: PTypeH, F: PTypeH, G: PTypeH, H: PTypeH, I: PTypeH, J: PTypeH, K: PTypeH, L: PTypeH, M: PTypeH, N: PTypeH, O: PTypeH, P: PTypeH, Q: PTypeH] = new PTypeH[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q)] {
    def get(ptf: PTypeFamily) = {
      ptf.tuple17(implicitly[PTypeH[A]].get(ptf), implicitly[PTypeH[B]].get(ptf), implicitly[PTypeH[C]].get(ptf), implicitly[PTypeH[D]].get(ptf), implicitly[PTypeH[E]].get(ptf), implicitly[PTypeH[F]].get(ptf), implicitly[PTypeH[G]].get(ptf), implicitly[PTypeH[H]].get(ptf), implicitly[PTypeH[I]].get(ptf), implicitly[PTypeH[J]].get(ptf), implicitly[PTypeH[K]].get(ptf), implicitly[PTypeH[L]].get(ptf), implicitly[PTypeH[M]].get(ptf), implicitly[PTypeH[N]].get(ptf), implicitly[PTypeH[O]].get(ptf), implicitly[PTypeH[P]].get(ptf), implicitly[PTypeH[Q]].get(ptf))
    }
  }

  implicit def tuple18[A: PTypeH, B: PTypeH, C: PTypeH, D: PTypeH, E: PTypeH, F: PTypeH, G: PTypeH, H: PTypeH, I: PTypeH, J: PTypeH, K: PTypeH, L: PTypeH, M: PTypeH, N: PTypeH, O: PTypeH, P: PTypeH, Q: PTypeH, R: PTypeH] = new PTypeH[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R)] {
    def get(ptf: PTypeFamily) = {
      ptf.tuple18(implicitly[PTypeH[A]].get(ptf), implicitly[PTypeH[B]].get(ptf), implicitly[PTypeH[C]].get(ptf), implicitly[PTypeH[D]].get(ptf), implicitly[PTypeH[E]].get(ptf), implicitly[PTypeH[F]].get(ptf), implicitly[PTypeH[G]].get(ptf), implicitly[PTypeH[H]].get(ptf), implicitly[PTypeH[I]].get(ptf), implicitly[PTypeH[J]].get(ptf), implicitly[PTypeH[K]].get(ptf), implicitly[PTypeH[L]].get(ptf), implicitly[PTypeH[M]].get(ptf), implicitly[PTypeH[N]].get(ptf), implicitly[PTypeH[O]].get(ptf), implicitly[PTypeH[P]].get(ptf), implicitly[PTypeH[Q]].get(ptf), implicitly[PTypeH[R]].get(ptf))
    }
  }

  implicit def tuple19[A: PTypeH, B: PTypeH, C: PTypeH, D: PTypeH, E: PTypeH, F: PTypeH, G: PTypeH, H: PTypeH, I: PTypeH, J: PTypeH, K: PTypeH, L: PTypeH, M: PTypeH, N: PTypeH, O: PTypeH, P: PTypeH, Q: PTypeH, R: PTypeH, S: PTypeH] = new PTypeH[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S)] {
    def get(ptf: PTypeFamily) = {
      ptf.tuple19(implicitly[PTypeH[A]].get(ptf), implicitly[PTypeH[B]].get(ptf), implicitly[PTypeH[C]].get(ptf), implicitly[PTypeH[D]].get(ptf), implicitly[PTypeH[E]].get(ptf), implicitly[PTypeH[F]].get(ptf), implicitly[PTypeH[G]].get(ptf), implicitly[PTypeH[H]].get(ptf), implicitly[PTypeH[I]].get(ptf), implicitly[PTypeH[J]].get(ptf), implicitly[PTypeH[K]].get(ptf), implicitly[PTypeH[L]].get(ptf), implicitly[PTypeH[M]].get(ptf), implicitly[PTypeH[N]].get(ptf), implicitly[PTypeH[O]].get(ptf), implicitly[PTypeH[P]].get(ptf), implicitly[PTypeH[Q]].get(ptf), implicitly[PTypeH[R]].get(ptf), implicitly[PTypeH[S]].get(ptf))
    }
  }

  implicit def tuple20[A: PTypeH, B: PTypeH, C: PTypeH, D: PTypeH, E: PTypeH, F: PTypeH, G: PTypeH, H: PTypeH, I: PTypeH, J: PTypeH, K: PTypeH, L: PTypeH, M: PTypeH, N: PTypeH, O: PTypeH, P: PTypeH, Q: PTypeH, R: PTypeH, S: PTypeH, T: PTypeH] = new PTypeH[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T)] {
    def get(ptf: PTypeFamily) = {
      ptf.tuple20(implicitly[PTypeH[A]].get(ptf), implicitly[PTypeH[B]].get(ptf), implicitly[PTypeH[C]].get(ptf), implicitly[PTypeH[D]].get(ptf), implicitly[PTypeH[E]].get(ptf), implicitly[PTypeH[F]].get(ptf), implicitly[PTypeH[G]].get(ptf), implicitly[PTypeH[H]].get(ptf), implicitly[PTypeH[I]].get(ptf), implicitly[PTypeH[J]].get(ptf), implicitly[PTypeH[K]].get(ptf), implicitly[PTypeH[L]].get(ptf), implicitly[PTypeH[M]].get(ptf), implicitly[PTypeH[N]].get(ptf), implicitly[PTypeH[O]].get(ptf), implicitly[PTypeH[P]].get(ptf), implicitly[PTypeH[Q]].get(ptf), implicitly[PTypeH[R]].get(ptf), implicitly[PTypeH[S]].get(ptf), implicitly[PTypeH[T]].get(ptf))
    }
  }

  implicit def tuple21[A: PTypeH, B: PTypeH, C: PTypeH, D: PTypeH, E: PTypeH, F: PTypeH, G: PTypeH, H: PTypeH, I: PTypeH, J: PTypeH, K: PTypeH, L: PTypeH, M: PTypeH, N: PTypeH, O: PTypeH, P: PTypeH, Q: PTypeH, R: PTypeH, S: PTypeH, T: PTypeH, U: PTypeH] = new PTypeH[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U)] {
    def get(ptf: PTypeFamily) = {
      ptf.tuple21(implicitly[PTypeH[A]].get(ptf), implicitly[PTypeH[B]].get(ptf), implicitly[PTypeH[C]].get(ptf), implicitly[PTypeH[D]].get(ptf), implicitly[PTypeH[E]].get(ptf), implicitly[PTypeH[F]].get(ptf), implicitly[PTypeH[G]].get(ptf), implicitly[PTypeH[H]].get(ptf), implicitly[PTypeH[I]].get(ptf), implicitly[PTypeH[J]].get(ptf), implicitly[PTypeH[K]].get(ptf), implicitly[PTypeH[L]].get(ptf), implicitly[PTypeH[M]].get(ptf), implicitly[PTypeH[N]].get(ptf), implicitly[PTypeH[O]].get(ptf), implicitly[PTypeH[P]].get(ptf), implicitly[PTypeH[Q]].get(ptf), implicitly[PTypeH[R]].get(ptf), implicitly[PTypeH[S]].get(ptf), implicitly[PTypeH[T]].get(ptf), implicitly[PTypeH[U]].get(ptf))
    }
  }

  implicit def tuple22[A: PTypeH, B: PTypeH, C: PTypeH, D: PTypeH, E: PTypeH, F: PTypeH, G: PTypeH, H: PTypeH, I: PTypeH, J: PTypeH, K: PTypeH, L: PTypeH, M: PTypeH, N: PTypeH, O: PTypeH, P: PTypeH, Q: PTypeH, R: PTypeH, S: PTypeH, T: PTypeH, U: PTypeH, V: PTypeH] = new PTypeH[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V)] {
    def get(ptf: PTypeFamily) = {
      ptf.tuple22(implicitly[PTypeH[A]].get(ptf), implicitly[PTypeH[B]].get(ptf), implicitly[PTypeH[C]].get(ptf), implicitly[PTypeH[D]].get(ptf), implicitly[PTypeH[E]].get(ptf), implicitly[PTypeH[F]].get(ptf), implicitly[PTypeH[G]].get(ptf), implicitly[PTypeH[H]].get(ptf), implicitly[PTypeH[I]].get(ptf), implicitly[PTypeH[J]].get(ptf), implicitly[PTypeH[K]].get(ptf), implicitly[PTypeH[L]].get(ptf), implicitly[PTypeH[M]].get(ptf), implicitly[PTypeH[N]].get(ptf), implicitly[PTypeH[O]].get(ptf), implicitly[PTypeH[P]].get(ptf), implicitly[PTypeH[Q]].get(ptf), implicitly[PTypeH[R]].get(ptf), implicitly[PTypeH[S]].get(ptf), implicitly[PTypeH[T]].get(ptf), implicitly[PTypeH[U]].get(ptf), implicitly[PTypeH[V]].get(ptf))
    }
  }

}
