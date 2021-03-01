import os
import subprocess
from typing import List
import typer
import shutil

from carte_cli.utils.file_io import read_json, write_json

CARTE_FRONTEND_REPO = "https://github.com/carte-data/carte-frontend.git"
APP_NAME = "my-super-cli-app"


def run_command(args: List[str]):
    subprocess.run(args)


def clone_repo(target: str):
    run_command(["git", "clone", "--depth", "1", CARTE_FRONTEND_REPO, target])


def remove_origin():
    run_command(["git", "remote", "rm", "origin"])


def create_frontend_dir(target: str, init_admin: bool = True, sample_data: bool = True):
    typer.echo("Cloning front end template...")
    clone_repo(target)
    os.chdir(target)
    remove_origin()

    typer.echo("Applying settings...")
    apply_settings(target, init_admin, sample_data)
    typer.echo("Done!")


def apply_settings(target: str, init_admin: bool, sample_data: bool):
    if not init_admin:
        shutil.rmtree(os.path.join("public", "admin"))
    if not sample_data:
        shutil.rmtree(os.path.join("data", "datasets", "postgres"))

    package_json_path = "package.json"
    package_json = read_json(package_json_path)
    package_json["name"] = target
    if not init_admin:
        package_json["dependencies"].pop("netlify-cms-app", None)

    write_json(package_json, package_json_path, indent=2)
    run_command(["git", "add", "package.json"])
    run_command(["git", "commit", "-m", "Initialise Carte"])
