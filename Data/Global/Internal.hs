{-
The code in this file was obtained from the safe-globals package,
version 0.1.1 which came to me under the following license:

Copyright (c) Keegan McAllister 2011

All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions
are met:
1. Redistributions of source code must retain the above copyright
   notice, this list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright
   notice, this list of conditions and the following disclaimer in the
   documentation and/or other materials provided with the distribution.
3. Neither the name of the author nor the names of his contributors
   may be used to endorse or promote products derived from this software
   without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS ``AS IS''
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED.  IN NO EVENT SHALL THE AUTHORS OR CONTRIBUTORS BE LIABLE FOR ANY
DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
-}
{-# LANGUAGE
    TemplateHaskell
  , CPP #-}
module Data.Global.Internal where

import Control.Monad
import Language.Haskell.TH
import Language.Haskell.TH.Syntax
import System.IO.Unsafe ( unsafePerformIO )

-- template-haskell 2.6.0.0
--
-- data InlineSpec
--   = InlineSpec Bool                 -- False: no inline; True: inline
--                Bool                 -- False: fun-like; True: constructor-like
--                (Maybe (Bool, Int))  -- False: before phase; True: from phase
--   deriving( Show, Eq, Data, Typeable )
--
-- data Pragma = InlineP     Name InlineSpec
--             | SpecialiseP Name Type (Maybe InlineSpec)
--         deriving( Show, Eq, Data, Typeable )

-- template-haskell 2.9.0.0
--
-- data Pragma = InlineP         Name Inline RuleMatch Phases
--             | SpecialiseP     Name Type (Maybe Inline) Phases
--             | SpecialiseInstP Type
--             | RuleP           String [RuleBndr] Exp Exp Phases
--             | AnnP            AnnTarget Exp
--         deriving( Show, Eq, Data, Typeable )
--
-- data Inline = NoInline
--             | Inline
--             | Inlinable
--             deriving (Show, Eq, Data, Typeable)
--
-- data RuleMatch = ConLike
--                | FunLike
--                deriving (Show, Eq, Data, Typeable)
--
-- data Phases = AllPhases
--             | FromPhase Int
--             | BeforePhase Int
--             deriving (Show, Eq, Data, Typeable)

polymorphic :: Type -> Bool
polymorphic (ForallT _ _ _) = True
polymorphic (VarT   _)      = True
{-
polymorphic (ConT   _)      = False
polymorphic (TupleT _)      = False
polymorphic ArrowT          = False
polymorphic ListT           = False
-}
polymorphic (AppT s t) = polymorphic s || polymorphic t
polymorphic (SigT t _) = polymorphic t
polymorphic _ = False

{-  -- as seen in template-haskell 2.6.0.0
data Type = ForallT [TyVarBndr] Cxt Type  -- ^ @forall <vars>. <ctxt> -> <type>@
          | VarT Name                     -- ^ @a@
          | ConT Name                     -- ^ @T@
          | TupleT Int                    -- ^ @(,), (,,), etc.@
          | UnboxedTupleT Int             -- ^ @(#,#), (#,,#), etc.@
          | ArrowT                        -- ^ @->@
          | ListT                         -- ^ @[]@
          | AppT Type Type                -- ^ @T a b@
          | SigT Type Kind                -- ^ @t :: k@
      deriving( Show, Eq, Data, Typeable )

  as seen in template-haskell 2.10.0.0
data Type = ForallT [TyVarBndr] Cxt Type  -- ^ @forall \<vars\>. \<ctxt\> -> \<type\>@
          | AppT Type Type                -- ^ @T a b@
          | SigT Type Kind                -- ^ @t :: k@
          | VarT Name                     -- ^ @a@
          | ConT Name                     -- ^ @T@
          | PromotedT Name                -- ^ @'T@

          -- See Note [Representing concrete syntax in types]
          | TupleT Int                    -- ^ @(,), (,,), etc.@
          | UnboxedTupleT Int             -- ^ @(#,#), (#,,#), etc.@
          | ArrowT                        -- ^ @->@
          | EqualityT                     -- ^ @~@
          | ListT                         -- ^ @[]@
          | PromotedTupleT Int            -- ^ @'(), '(,), '(,,), etc.@
          | PromotedNilT                  -- ^ @'[]@
          | PromotedConsT                 -- ^ @(':)@
          | StarT                         -- ^ @*@
          | ConstraintT                   -- ^ @Constraint@
          | LitT TyLit                    -- ^ @0,1,2, etc.@
      deriving( Show, Eq, Ord, Data, Typeable, Generic )
-}

declare :: Q Type -> Q Exp -> String -> Q [Dec]
declare mty newRef nameStr = do
    let name = mkName nameStr
    ty <- mty
    declareName ty newRef name

declareName :: Type -> Q Exp -> Name -> Q [Dec]
declareName ty newRef name = do
    when (polymorphic ty) $
        error ("Data.Global: cannot declare ref of polymorphic type " ++
               show (ppr ty))

    body <- [| unsafePerformIO $newRef |]

    return [
        SigD name ty
      , ValD (VarP name) (NormalB body) []
#if MIN_VERSION_template_haskell(2,8,0)
      , PragmaD (InlineP name NoInline FunLike AllPhases) ]
#else
      , PragmaD (InlineP name (InlineSpec False False Nothing)) ]
#endif

declareRef :: Name -> Q Exp -> String -> Q Type -> Q [Dec]
declareRef refTy newRef nameStr mty
    = declare (appT (conT refTy) mty) newRef nameStr

-- -- | The type of macros for declaring variables.
-- type Declare     = String -> Q Type          -> Q [Dec]
--
-- -- | Declare an empty @'MVar'@.
-- --
-- -- >declareEmptyMVar "foo" [t| Char |]
-- declareEmptyMVar  :: Declare
-- declareEmptyMVar  = declareRef ''MVar  [| newEmptyMVar    |]

