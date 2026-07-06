"""The base-to-base step family — recovers the transform between two arms' bases from a
marker both their (already-calibrated) wrist cameras see. Its dependency is explicit: the
two partner cameras must be accepted first, so the factory raises StepError until they are
(the pipeline's `depends_on` enforces ordering; this is the engine-side guard).
"""
from __future__ import annotations

from ..session import BaseToBaseSession
from .base import StepContext, StepError, register


@register("base_to_base", "b2b")
def _build(item, ctx: StepContext) -> BaseToBaseSession:
    ra = ctx.accepted.get(item.partner_camera)
    rb = ctx.accepted.get(item.secondary_camera)
    if ra is None or rb is None:
        missing = [c for c, r in ((item.partner_camera, ra), (item.secondary_camera, rb)) if r is None]
        raise StepError(f"calibrate + accept these cameras first: {', '.join(missing)}")
    pa = next(p for p in ctx.plan_items if p.camera_link == item.partner_camera)
    pb = next(p for p in ctx.plan_items if p.camera_link == item.secondary_camera)
    return BaseToBaseSession(
        item, ctx.target, ctx.K,
        ctx.chain_provider(pa.flange_link), ctx.chain_provider(pb.flange_link),
        ctx.bridge_factory(pa), ctx.bridge_factory(pb),
        ra.T_optical, rb.T_optical,
        # the FK corrections each hand-eye was solved WITH — X and offsets travel together
        fk_a=getattr(ra, "fk_offsets", None), fk_b=getattr(rb, "fk_offsets", None),
        # the task-space agreement gate reuses the tip-landing mm budget; the observability gate
        # reuses the same per-DOF σ limits as the supervised cameras (operator-tuned in cell.yaml).
        accept_agreement_mm=ctx.accept_tip_mm,
        accept_rot_sigma_deg=ctx.accept_rot_sigma_deg,
        accept_trans_sigma_mm=ctx.accept_trans_sigma_mm,
    )
