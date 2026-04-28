"""
Generador automático de documentación técnica del proyecto.

Module: tools.generate_docs
Purpose: Lee todos los archivos .py del proyecto, extrae los docstrings de
    módulos, funciones y clases, y genera un documento Markdown con la
    estructura completa del proyecto, las dependencias entre módulos y
    un índice navegable.
Input: Todos los archivos .py del proyecto (src/, dashboard/, run_jobs.py)
Output: docs/TECHNICAL_REFERENCE.md
Config: Ninguna
Used by: Desarrolladores para generar/actualizar la documentación técnica.
    También puede usarse como fuente para un sistema RAG o para subir
    como contexto a Claude Projects.

Uso:
    python tools/generate_docs.py
    python tools/generate_docs.py --output docs/custom_name.md
"""
import ast
import os
import sys
import argparse
from pathlib import Path
from datetime import datetime


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUTPUT = PROJECT_ROOT / "docs" / "TECHNICAL_REFERENCE.md"

# Directorios a escanear (relativos a PROJECT_ROOT)
SCAN_DIRS = ["src", "dashboard"]
SCAN_FILES = ["run_jobs.py"]

# Archivos/directorios a excluir
EXCLUDE = {"__pycache__", ".venv", "venv", "node_modules", ".git", "assets"}


def main():
    parser = argparse.ArgumentParser(description="Genera documentación técnica del proyecto")
    parser.add_argument("--output", type=str, default=str(DEFAULT_OUTPUT),
                        help="Ruta del archivo Markdown de salida")
    args = parser.parse_args()

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"Escaneando proyecto: {PROJECT_ROOT}")

    # Recopilar todos los archivos .py
    py_files = _collect_py_files()
    print(f"Encontrados {len(py_files)} archivos .py")

    # Extraer documentación de cada archivo
    modules = []
    for filepath in sorted(py_files):
        module_doc = _extract_module_doc(filepath)
        if module_doc:
            modules.append(module_doc)

    # Generar Markdown
    md_content = _generate_markdown(modules)

    # Escribir archivo
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(md_content)

    print(f"Documentación generada: {output_path}")
    print(f"  - {len(modules)} módulos documentados")
    total_funcs = sum(len(m["functions"]) for m in modules)
    print(f"  - {total_funcs} funciones documentadas")


def _collect_py_files() -> list[Path]:
    """Recopila todos los archivos .py del proyecto."""
    files = []

    # Archivos en la raíz
    for filename in SCAN_FILES:
        filepath = PROJECT_ROOT / filename
        if filepath.exists():
            files.append(filepath)

    # Directorios recursivos
    for dirname in SCAN_DIRS:
        dirpath = PROJECT_ROOT / dirname
        if not dirpath.exists():
            continue
        for root, dirs, filenames in os.walk(dirpath):
            # Excluir directorios
            dirs[:] = [d for d in dirs if d not in EXCLUDE]
            for filename in filenames:
                if filename.endswith(".py") and filename != "__init__.py":
                    files.append(Path(root) / filename)

    return files


def _extract_module_doc(filepath: Path) -> dict | None:
    """Extrae la documentación de un archivo .py usando el AST.

    Returns:
        dict con: path, relative_path, module_docstring, functions[]
        Cada función tiene: name, docstring, args, returns, lineno
    """
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            source = f.read()
        tree = ast.parse(source)
    except (SyntaxError, UnicodeDecodeError) as e:
        print(f"  ⚠ Error parseando {filepath}: {e}")
        return None

    relative_path = filepath.relative_to(PROJECT_ROOT)

    module_doc = {
        "path": str(filepath),
        "relative_path": str(relative_path),
        "module_docstring": ast.get_docstring(tree) or "",
        "functions": [],
        "constants": [],
    }

    for node in ast.walk(tree):
        # Funciones (incluyendo las de nivel de módulo y las de dentro de clases)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            func_doc = {
                "name": node.name,
                "docstring": ast.get_docstring(node) or "",
                "lineno": node.lineno,
                "args": _extract_args(node),
                "returns": _extract_return_annotation(node),
                "decorators": [_get_decorator_name(d) for d in node.decorator_list],
            }
            module_doc["functions"].append(func_doc)

        # Constantes a nivel de módulo (UPPER_CASE)
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id.isupper():
                    module_doc["constants"].append({
                        "name": target.id,
                        "lineno": node.lineno,
                    })

    return module_doc


def _extract_args(node: ast.FunctionDef) -> list[str]:
    """Extrae los nombres y anotaciones de los argumentos de una función."""
    args = []
    for arg in node.args.args:
        name = arg.arg
        if name == "self":
            continue
        annotation = ""
        if arg.annotation:
            annotation = f": {ast.unparse(arg.annotation)}"
        args.append(f"{name}{annotation}")
    return args


def _extract_return_annotation(node: ast.FunctionDef) -> str:
    """Extrae la anotación de retorno de una función."""
    if node.returns:
        return ast.unparse(node.returns)
    return ""


def _get_decorator_name(decorator) -> str:
    """Extrae el nombre de un decorador."""
    if isinstance(decorator, ast.Name):
        return decorator.id
    elif isinstance(decorator, ast.Attribute):
        return ast.unparse(decorator)
    elif isinstance(decorator, ast.Call):
        return ast.unparse(decorator.func)
    return ""


def _generate_markdown(modules: list[dict]) -> str:
    """Genera el documento Markdown completo."""
    lines = []

    # Cabecera
    lines.append("# Referencia Técnica — reporting_equipo_directivo")
    lines.append("")
    lines.append(f"Generado automáticamente el {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    lines.append("")
    lines.append("Este documento describe la estructura completa del proyecto, cada módulo,")
    lines.append("cada función con sus parámetros, dependencias y lógica de negocio.")
    lines.append("Puede usarse como contexto para consultas con IA (Claude Projects, RAG, etc.).")
    lines.append("")

    # Índice
    lines.append("---")
    lines.append("")
    lines.append("## Índice de Módulos")
    lines.append("")
    for m in modules:
        n_funcs = len(m["functions"])
        first_line = m["module_docstring"].split("\n")[0] if m["module_docstring"] else ""
        anchor = m["relative_path"].replace("/", "").replace("\\", "").replace(".", "").replace("_", "-").lower()
        lines.append(f"- [{m['relative_path']}](#{anchor}) — {first_line} ({n_funcs} funciones)")
    lines.append("")

    # Mapa de dependencias
    lines.append("---")
    lines.append("")
    lines.append("## Mapa de Dependencias")
    lines.append("")
    lines.append("```")
    lines.append("run_jobs.py")
    lines.append("  ├── src/etl/import_export.py      (volcado export → HOJA EXISTENCIAS)")
    lines.append("  ├── src/etl/etl_existencias.py     (extracción + transformación)")
    lines.append("  ├── src/alerts/rules_existencias.py (evaluación alertas A1–A10)")
    lines.append("  ├── src/utils/persistence.py        (escritura históricos CSV)")
    lines.append("  ├── src/utils/email_sender.py       (envío emails SMTP)")
    lines.append("  ├── src/utils/config_loader.py      (carga settings.yaml + .env)")
    lines.append("  └── src/utils/logger.py             (logging centralizado)")
    lines.append("")
    lines.append("dashboard/Control_de_Existencias.py   (panel general)")
    lines.append("  ├── pages/1_Alertas_Activas.py")
    lines.append("  ├── pages/2_Detalle_Articulo.py")
    lines.append("  ├── pages/3_Evolucion.py")
    lines.append("  ├── pages/4_Proveedores.py")
    lines.append("  ├── pages/5_Admin.py")
    lines.append("  └── styles.py                       (estilos corporativos)")
    lines.append("  Todas las páginas usan:")
    lines.append("    ├── src/utils/config_loader.py")
    lines.append("    ├── src/etl/etl_existencias.py")
    lines.append("    └── src/alerts/rules_existencias.py")
    lines.append("```")
    lines.append("")

    # Detalle de cada módulo
    lines.append("---")
    lines.append("")
    for m in modules:
        lines.append(f"## {m['relative_path']}")
        lines.append("")

        if m["module_docstring"]:
            lines.append("### Descripción del módulo")
            lines.append("")
            lines.append("```")
            lines.append(m["module_docstring"])
            lines.append("```")
            lines.append("")

        # Constantes
        if m["constants"]:
            lines.append("### Constantes")
            lines.append("")
            for const in m["constants"]:
                lines.append(f"- `{const['name']}` (línea {const['lineno']})")
            lines.append("")

        # Funciones
        if m["functions"]:
            lines.append("### Funciones")
            lines.append("")
            for func in m["functions"]:
                decorators = ""
                if func["decorators"]:
                    decorators = " ".join(f"`@{d}`" for d in func["decorators"])
                    decorators = f" {decorators}"

                args_str = ", ".join(func["args"])
                returns_str = f" → {func['returns']}" if func["returns"] else ""
                lines.append(f"#### `{func['name']}({args_str}){returns_str}`{decorators}")
                lines.append("")
                lines.append(f"*Línea {func['lineno']}*")
                lines.append("")
                if func["docstring"]:
                    lines.append(func["docstring"])
                    lines.append("")
                lines.append("---")
                lines.append("")

    # Pie
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append(f"*Documento generado por `tools/generate_docs.py` el {datetime.now().strftime('%d/%m/%Y %H:%M')}*")

    return "\n".join(lines)


if __name__ == "__main__":
    main()
