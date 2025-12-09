# Make File
# Author: S Cotton
# Date: 2025-10-03
# Version: 1.0
# Description: Makefile for building and cleaning the project

.DEFAULT_GOAL := run

# setup depends on requirements.txt
setup: requirements.txt
	@read -p 'Enter virtual environment name (default: .venv): ' VENV_NAME; \
	if [ -z "$$VENV_NAME" ]; then \
echo "Using default virtual environment name .venv"; \
		VENV_NAME=".venv"; \
	fi; \
	echo "Setting up the environment..."; \
	if [ ! -d ".git" ]; then \
		echo "Initializing git repository..."; \
		git init; \
	fi; \
	python3 -m venv $$VENV_NAME; \
	@$(ACTIVATE_LINUX); \
	. $$VENV_NAME/bin/activate && pip install -r requirements.txt

activate: requirements.txt
	@read -p 'Enter virtual environment name (default: .venv): ' VENV_NAME; \
	if [ -z "$$VENV_NAME" ]; then \
		VENV_NAME=".venv"; \
	fi; \
	if [ ! -d "$$VENV_NAME" ]; then \
		echo "Virtual environment '$$VENV_NAME' does not exist."; \
		read -p "Create new virtual environment? (Y/N): " CREATE_VENV; \
		if [ "$$CREATE_VENV" = "Y" ] || [ "$$CREATE_VENV" = "y" ]; then \
			echo "Creating virtual environment $$VENV_NAME..."; \
			python3 -m venv $$VENV_NAME; \
			if [ $$? -eq 0 ]; then \
				echo "Virtual environment successfully created"; \
				. $$VENV_NAME/bin/activate && pip install -r requirements.txt; \
			else \
				echo "Virtual environment creation failed"; \
				exit 1; \
			fi; \
		else \
			echo "Virtual environment creation cancelled"; \
			exit 1; \
		fi; \
	else \
		echo "Activating virtual environment $$VENV_NAME..."; \
		. $$VENV_NAME/bin/activate && pip install -r requirements.txt; \
	fi

.PHONY: clean
clean:
	echo "Cleaning up..."; \
	if [ -d "build" ]; then rm -rf build; fi; \
	if [ -d "dist" ]; then rm -rf dist; fi; \
	if [ -d "__pycache__" ]; then rm -rf __pycache__; fi
	if [ -d "__init__.py" ]; then rm -rf __init__.py; fi
	if [ -d ".eggs" ]; then rm -rf .eggs; fi



