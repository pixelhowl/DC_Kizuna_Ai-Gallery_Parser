"""Kotlinside related function"""
import os
import time

import jpype

from . import logging, paths, strings

os.environ["JAVA_HOME"] = paths.CLASS_PATH

# pylint: disable=too-many-function-args, broad-except
MAX_TRIAL = 3


def run_once(f):

    def wrapper(*args, **kwargs):
        if not wrapper.has_run:
            wrapper.has_run = True
            return f(*args, **kwargs)

    wrapper.has_run = False
    return wrapper


@run_once
def jvm_init():
    jpype.startJVM(jpype.getDefaultJVMPath(),
                   strings.JAVA_PATH_ARGS.format(CLASS_PATH=paths.CLASS_PATH),
                   convertStrings=True)


def get_auth():
    jvm_init()
    kotlininside = jpype.JPackage(strings.KOTLININSIDE_PACKAGE_NAME)
    user = kotlininside.session.user.Anonymous
    inside = kotlininside.KotlinInside
    http = kotlininside.http.DefaultHttpClient

    _ = inside.createInstance(
        user(strings.KOTLININSIDE_USERNAME, strings.KOTLININSIDE_PASSWORD),
        http(True, True))
    auth = kotlininside.security.Auth()
    return auth


def generate_app_id(auth):
    for i in range(MAX_TRIAL):
        try:
            hased_app_key = auth.generateHashedAppKey()
            app_id = auth.fetchAppId(hased_app_key)
        except Exception as e:
            logging.LOGGER.info("Trial: %s, error: %s", i, e)
            time.sleep(60)
        return app_id

    raise RuntimeError("Cannot handle")


def jvm_shutdown():
    jpype.shutdownJVM(jpype.getDefaultJVMPath())
