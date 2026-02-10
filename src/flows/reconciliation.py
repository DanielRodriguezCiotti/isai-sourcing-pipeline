from prefect import flow

from src.tasks import (
    companies_reconciliation,
    founders_reconciliation,
    funding_rounds_reconciliation,
)


@flow(name="reconciliation-flow")
def reconciliation_flow(domains: list[str]):
    companies_reconciliation(domains)
    founders_reconciliation(domains)
    funding_rounds_reconciliation(domains)


domains = [
    "zwsoft.com",
    "propelsoftware.com",
    "proteantecs.com",
    "etherealmachines.com",
    "colabsoftware.com",
    "monolithai.com",
    "caddi.com",
    "physicsx.ai",
    "dspace.com",
    "apriori.com",
    "simscale.com",
    "luminarycloud.com",
    "allspice.io",
    "synera.io",
    "oneclicklca.com",
    "aras.com",
    "techsoft3d.com",
    "highbyte.com",
    "quilter.ai",
    "vayavyalabs.com",
    "cevotec.com",
    "bluespec.com",
    "inventables.com",
    "flux.ai",
    "minviro.com",
    "ntop.com",
    "conceptsnrec.com",
    "foundationegi.com",
    "zoo.dev",
    "averna.com",
    "efabless.com",
    "circuitmind.io",
    "gtisoft.com",
    "durolabs.co",
    "riiico.com",
    "pcbstator.com",
    "simyog.com",
    "toffeex.com",
    "adtechnology.com",
    "hysopt.com",
    "beyondmath.com",
    "nullspaceinc.com",
    "aletiq.com",
    "toolpath.com",
    "hirebotics.com",
    "delogue.com",
    "madesmarter.uk",
    "getleo.ai",
    "demxs.com",
    "comsol.com",
    "thalia-da.com",
]
reconciliation_flow(domains)
