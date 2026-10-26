"""User entry points: inspect an export, run the small example, use the full SUT."""
import argparse
import json
from pathlib import Path
import sys

from .inputs import Projections
from .shocks import apply_mix_shock
from .demo import demo_database
from .results import export_run
from .preparation import prepare_database
from .download import download_database, DEFAULT_DIRECTORY


def main(argv=None):
    parser = argparse.ArgumentParser(description="GCAM export -> explicit mix shares -> MARIO coefficient shock")
    commands = parser.add_subparsers(dest="command", required=True)
    dl = commands.add_parser("download", help="Download and verify fixed extended EXIOBASE v1.3.0")
    dl.add_argument("--output", type=Path, default=DEFAULT_DIRECTORY)
    prep = commands.add_parser("prepare", help="Prepare the full database with EU-12/EU-15 regions once")
    inspect = commands.add_parser("inspect", help="Export the selected GCAM rows and their interpretation; no database needed")
    demo = commands.add_parser("demo", help="Apply real GCAM projections to a small illustrative MARIO database")
    run = commands.add_parser("run", help="Apply GCAM mixes to the extended EXIOBASE database")
    for command in [prep, run]:
        command.add_argument("--prepared", type=Path, default=Path("models/prepared"))
        command.add_argument("--source", type=Path, help="Existing source coefficient-Parquet directory")
        command.add_argument("--offline", action="store_true", help="Never download; require local files")
    for command in [inspect, demo, run]:
        command.add_argument("--scenario", default="NDC_LTT_CBAM")
        command.add_argument("--year", type=int, default=2030)
        command.add_argument("--electricity-export", type=Path, help="GCAM CSV in physical output units")
        command.add_argument("--steel-export", type=Path, help="GCAM steel technology CSV in physical output units")
        command.add_argument("--profile", choices=["eu-clusters", "eu-countries"], default="eu-clusters")
        command.add_argument("--region-map", type=Path, help="Explicit GCAM-region -> MARIO-region-list JSON")
        command.add_argument("--only", choices=["both", "electricity", "steel"], default="both")
        command.add_argument("--output", type=Path)
        command.add_argument("--overwrite", action="store_true")
    args = parser.parse_args(argv)
    try:
        if args.command == "download":
            download_database(args.output)
            return 0
        import mario
        mario.set_log_verbosity("error")
        if args.command == "prepare":
            db, manifest = prepare_database(args.prepared, source=args.source, offline=args.offline)
            print(f"Prepared {len(db.get_index('Region'))} regions: {args.prepared.resolve()}")
            return 0
        mapping = json.loads(args.region_map.read_text()) if args.region_map else None
        kinds = ("electricity", "steel") if args.only == "both" else (args.only,)
        projections = Projections(electricity_export=args.electricity_export, steel_export=args.steel_export)
        selected = projections.select(args.scenario, args.year, profile=args.profile, region_mapping=mapping, kinds=kinds)
        output = args.output or Path("outputs")/f"{args.command}_{args.scenario}_{args.year}_{args.only}"
        if output.exists() and any(output.iterdir()) and not args.overwrite:
            raise ValueError(f"{output} is not empty; choose another --output or use --overwrite")
        if args.command == "inspect":
            output.mkdir(parents=True, exist_ok=True)
            selected.trace.to_csv(output/"input_interpretation.csv", index=False)
            (output/"selection.json").write_text(json.dumps(selected.as_dict(), indent=2)+"\n")
            print(f"GCAM rows, mapped names, physical totals and target shares: {output.resolve()}")
            return 0
        if args.command == "demo":
            db, manifest = demo_database(), None
            print("Using real GCAM projections with FICTIONAL teaching inventories; footprints are illustrative.", flush=True)
        else:
            db, manifest = prepare_database(args.prepared, source=args.source, offline=args.offline)
        name = f"{args.scenario}_{args.year}_{args.only}"
        report = apply_mix_shock(db, selected, name=name, electricity="electricity" in kinds, steel="steel" in kinds)
        result = export_run(db, [report], output, database_kind="demo" if args.command == "demo" else "extended-exiobase",
                            prepared_manifest=manifest, overwrite=args.overwrite)
        print(f"Results and input -> coefficient trace: {result}")
        return 0
    except (ValueError, OSError, KeyError) as exc:
        print(f"GCAM-MARIO: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
