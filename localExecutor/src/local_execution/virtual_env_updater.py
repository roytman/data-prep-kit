import subprocess
import sys


def uninstall(name: str):
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "uninstall", "-y", name])
        print("package uninstalled")
    except Exception as e:
        print(f"Exception installing package {e}")


class AbstractEnvUpdater:

    def install(self, package: str):
        raise ValueError("must be implemented by subclass")

    @staticmethod
    def check_installed(name: str):
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "show", "-q", name])
            print("package is installed")
        except Exception as e:
            print(f"Exception show package {e}")

    @staticmethod
    def uninstall(name: str):
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "uninstall", "-y", name])
            print("package uninstalled")
        except Exception as e:
            print(f"Exception installing package {e}")


class GitHubEnvUpdater(AbstractEnvUpdater):
    def __init__(
        self,
        gitrepo: str,
    ):
        """
        Updates virtual environment from a remote git repository
        :param gitrepo: the remote git repository
        """
        self.gitrepo = gitrepo

    def install(self, package: str):
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", self.gitrepo + package])
            print("package installed")
        except Exception as e:
            print(f"Exception installing package {e}")


class LocalRepositoryEnvUpdater(AbstractEnvUpdater):
    def __init__(
        self,
        gitrepo: str,
    ):
        """
        Updates virtual environment from a local git repository
        :param gitrepo: the local git repository
        """
        self.gitrepo = gitrepo

    def install(self, package: str):
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", "-e", self.gitrepo + package])
            print("package installed")
        except Exception as e:
            print(f"Exception installing package {e}")
