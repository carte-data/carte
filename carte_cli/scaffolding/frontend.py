import os
import subprocess
from typing import List
import typer
import shutil

from carte_cli.utils.file_io import read_yaml, write_yaml

CARTE_FRONTEND_REPO = "https://github.com/carte-data/carte-frontend.git"


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
        shutil.rmtree(os.path.join("content", "admin"))
    if not sample_data:
        shutil.rmtree(os.path.join("datasets", "postgres"))

    mkdocs_path = "mkdocs.yml"
    mkdocs_config = read_yaml(mkdocs_path)
    mkdocs_config["site_name"] = target
    mkdocs_config["repo_url"] = ""
    mkdocs_config["repo_name"] = ""
    mkdocs_config["site_url"] = ""

    write_yaml(mkdocs_config, mkdocs_path)
    run_command(["git", "add", mkdocs_path])
    run_command(["git", "commit", "-m", "Initialise Carte"])
