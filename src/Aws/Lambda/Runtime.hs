module Aws.Lambda.Runtime
  ( runLambda
  , runLambdaWithCache
  , Runtime.RunCallback
  , Runtime.LambdaResult(..)
  ) where

import Control.Exception.Safe.Checked
import Control.Monad (forever)
import qualified Network.HTTP.Client as Http

import qualified Aws.Lambda.Runtime.ApiInfo as ApiInfo
import qualified Aws.Lambda.Runtime.Common as Runtime
import qualified Aws.Lambda.Runtime.Context as Context
import qualified Aws.Lambda.Runtime.Environment as Environment
import qualified Aws.Lambda.Runtime.Error as Error
import qualified Aws.Lambda.Runtime.Publish as Publish

-- | Runs the user @haskell_lambda@ executable and posts back the
-- results. This is called from the layer's @main@ function.
runLambda :: Runtime.RunCallback -> IO ()
runLambda callback = do
  manager <- Http.newManager httpManagerSettings
  lambdaApi <- Environment.apiEndpoint `catch` variableNotSet
  forever $ do
    event     <- ApiInfo.fetchEvent manager lambdaApi `catch` errorParsing
    context   <- Context.initialize event `catch` errorParsing `catch` variableNotSet
    ((invokeAndRun callback manager lambdaApi event context
      `catch` \err -> Publish.parsingError err lambdaApi context manager)
      `catch` \err -> Publish.invocationError err lambdaApi context manager)
      `catch` \(err :: Error.EnvironmentVariableNotSet) -> Publish.runtimeInitError err lambdaApi context manager

runLambdaWithCache :: (Runtime.LambdaOptions -> a -> IO (Either String (Runtime.LambdaResult, a))) -> a -> IO ()
runLambdaWithCache callback initCache = do
  manager <- Http.newManager httpManagerSettings
  lambdaApi <- Environment.apiEndpoint `catch` variableNotSet
  let go cache =
          do event    <- ApiInfo.fetchEvent manager lambdaApi `catch` errorParsing
             context  <- Context.initialize event `catch` errorParsing `catch` variableNotSet
             newCache <- ((invokeAndRunWithCache callback manager lambdaApi event context cache
                           `catch` \err -> cache <$ Publish.parsingError err lambdaApi context manager)
                           `catch` \err -> cache <$ Publish.invocationError err lambdaApi context manager)
                           `catch` \(err :: Error.EnvironmentVariableNotSet) -> cache <$ Publish.runtimeInitError err lambdaApi context manager
             go newCache
  go initCache


httpManagerSettings :: Http.ManagerSettings
httpManagerSettings =
  -- We set the timeout to none, as AWS Lambda freezes the containers.
  Http.defaultManagerSettings
  { Http.managerResponseTimeout = Http.responseTimeoutNone
  }

invokeAndRun
  :: Throws Error.Invocation
  => Throws Error.EnvironmentVariableNotSet
  => Runtime.RunCallback
  -> Http.Manager
  -> String
  -> ApiInfo.Event
  -> Context.Context
  -> IO ()
invokeAndRun callback manager lambdaApi event context = do
  result    <- invokeWithCallback callback event context
  Publish.result result lambdaApi context manager
    `catch` \err -> Publish.invocationError err lambdaApi context manager

invokeAndRunWithCache
  :: Throws Error.Invocation
  => Throws Error.EnvironmentVariableNotSet
  => (Runtime.LambdaOptions -> a -> IO (Either String (Runtime.LambdaResult, a)))
  -> Http.Manager
  -> String
  -> ApiInfo.Event
  -> Context.Context
  -> a
  -> IO a
invokeAndRunWithCache callback manager lambdaApi event context cache = do
  (result, newCache) <- invokeWithCallbackWithCache callback event context cache
  Publish.result result lambdaApi context manager
    `catch` \err -> Publish.invocationError err lambdaApi context manager
  return newCache

invokeWithCallback
  :: Throws Error.Invocation
  => Throws Error.EnvironmentVariableNotSet
  => Runtime.RunCallback
  -> ApiInfo.Event
  -> Context.Context
  -> IO Runtime.LambdaResult
invokeWithCallback callback event context = do
  handlerName <- Environment.handlerName
  let lambdaOptions = Runtime.LambdaOptions
                      { eventObject = ApiInfo.event event
                      , contextObject = context
                      , functionHandler = handlerName
                      , executionUuid = ""  -- DirectCall doesnt use UUID
                      }
  result <- callback lambdaOptions
  case result of
    Left err ->
      throw $ Error.Invocation err
    Right value ->
      pure value

invokeWithCallbackWithCache
  :: Throws Error.Invocation
  => Throws Error.EnvironmentVariableNotSet
  => (Runtime.LambdaOptions -> a -> IO (Either String (Runtime.LambdaResult, a)))
  -> ApiInfo.Event
  -> Context.Context
  -> a
  -> IO (Runtime.LambdaResult, a)
invokeWithCallbackWithCache callback event context cache = do
  handlerName <- Environment.handlerName
  let lambdaOptions = Runtime.LambdaOptions
                      { eventObject = ApiInfo.event event
                      , contextObject = context
                      , functionHandler = handlerName
                      , executionUuid = ""  -- DirectCall doesnt use UUID
                      }
  result <- callback lambdaOptions cache
  case result of
    Left err ->
      throw $ Error.Invocation err
    Right value ->
      pure value

variableNotSet :: Error.EnvironmentVariableNotSet -> IO a
variableNotSet (Error.EnvironmentVariableNotSet env) =
  error ("Error initializing, variable not set: " <> env)

errorParsing :: Error.Parsing -> IO a
errorParsing Error.Parsing{..} =
  error ("Failed parsing " <> errorMessage <> ", got" <> actualValue)
