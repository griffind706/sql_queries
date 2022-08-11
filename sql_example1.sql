select
d.service_date
, rg.facilty_id
, rg.rooom_id
, rg.room_number
, hskp.sts_nm
, hskp.rsrc_id
, max(case when a.owner_sts_nm = 'active' then 1 else 0 end) as room_is_occupied_in
, max(case when a.asgn_lck_in = 'Y' thne 1 else 0 end) as room_is_lck_in
, max(coalesce(cf_ooi.room_cld_flr_in, 0)) as room_cld_flr_in
, max(coalesce(cf_ooi.cld_flr_end_dt_in, 0)) as cld_flr_end_dt_in
, max(coalesce(cf_ooi.room_is_out_of_invt_in, 0)) as room_is_out_of_invt_in
, max(coalesce(cf_ooi.out_of_invt_end_dt_in, 0)) as out_of_invt_end_dt_in
, max(coalesce(cf_ooi.cld_flr_end_late_in, 0)) as cld_flr_end_late_in
, max(coalesce(cf_ooi.cld_flr_start_early_in, 0)) as cld_flr_start_early_in
, rg.insert_dt as run_dt
, current_timestamp as insert_dt
, least(ifnull(rg.insert_dt, '2099-01-31 01:02:03.004'), ifnull(a.a_update_dt, '2099-01-31 01:02:03.004'), ifnull(cf_ooi.cf_ooi_update_dt, '2099-01-31 01:02:03.004'),) as oldest_upd_dt
from table_rg rg
left join (
	select
	clndr_dt
	from table_dt dt
	where dt.clndr_dt between current_date and current_date + 30
	group by 1) d

	left join (
		select
		d.clndr_dt as service_date
		, rao.asgn_own_id
		, ro_rsrc_own_id
		, rar.rsrc_asgn_req_id
		, ror.extnl_req_id as tc_id
		, ro_own_ds as own_name
		, rao_auto_asgn_rsrc_id as rsrc_id
		, ra.asgn_lck_in
		, rao.ownr_sts_nm
		, rao.ownr_strt_dts as chn_dts
		, rao.ownr_end_dts as cho_dts
		, least(max(rao.insert_dt), max(ror.insert_dt), max(rar.insert_dt), max(ra.insert_dt)) as a_update_dt
		from table_rao rao
		inner join table_dt d
			on d.clndr_dt between cast(rao.ownr_strt_dts as date) and dateadd(day, -1, cast(rao.ownr_end_dts as date))
			and d.clndr_dt between current_date and current_date + 30
		left join table_fc_typ_d ftd
			on rao.fac_id = ftd.ent_fac_id
		left join table_rsrc_own ro
			on rao.asgn_own_id = ro.asgn_own_id
		left join table_rsrc_own_ref ror
			on ro.rsrc_own_id = ror.rsrc_own_id
		left join table_rsrc_asgn_r rar
			on rao.asgn_own_id = rar.asgn_own_id
		left join table_rsrc_asgn ra
			on rar.asgn_req_id = ra.asgn_req_id
		where
			rao.auto_asgn is not null
		and rao.ownr_sts_nm in ('active', 'pending')
		and rao.asgn.ownr_typ.nm <> 'virtual'
		and ro.own_ds no_like 'closed%'
		and ftd.fac_id not in ('12', '34', '789')
		group by
		1,2,3,4,5,6,7,8,9,10,11) a
			on a.service_date = d.clndr_dt
			and a.rsrc_id = rg_room_id
		left join
			(
				select
					room_id
					, room_number
					, facilty_id
					, calendar_date
					, request_date
					, max(room_cld_flr_in) as room_cld_flr_in
					, max(cld_flr_end_dt_in) as cld_flr_end_dt_in
					, max(room_is_out_of_invt_in) as room_is_out_of_invt_in
					, min(out_of_invt_end_dt_in) as out_of_invt_end_dt_in
					, max(cld_flr_end_late_in) as cld_flr_end_late_in
					, max(cld_flr_start_early_in) as cld_flr_start_early_in
					, min(cf_ooi_inner_update_dt) as cf_ooi_update_dt
				from
					(
						select
							rs.room_id
							, rs.room_number
							, rs.facilty_id
							, d.clndr_dt as calendar_date
							, in_r.invt_req_typ_nm as request_type
							, in_r.invt_req_strt_dt as reqest_start_date
							, in_r.invt_req_end_dt as request_end_date
							, in_r.rsn_tx as request_reason
							, d.clndr_mo_mn as calendar_month
							, d.clndr_wk_dy_mn as calendar_dow
							, case when in_r.invtry_req_typ_nm = 'closed' then 1 else 0 end as room_cld_flr_in
							, case when in_r.invtry_req_typ_nm = 'closed' and cast(in_r.invt_req_end_dt as date) = d.clndr_dt then 1 else 0 end as cld_flr_end_dt_in
							, case when in_r.invtry_req_typ_nm = 'out' then 1 else 0 end as room_is_out_of_invt_in
							, case when in_r.invtry_req_typ_nm = 'out' and cast(in_r.invt_req_end_dt as date) = d.clndr_dt then 1 else 0 end as out_of_invt_end_dt_in
							, case when (cast(in_r.invt_req_end_dt as time) >= '16:00:00' and in_r.invt_req_typ_nm = 'closed') then 1 else 0 end as cld_flr_end_late_in
							, case when (cast(in_r.invt_req_strt_dt as time) <= '11:00:00' and in_r.invt_req_typ_nm = 'closed') then 1 else 0 end as cld_flr_start_early_in
							, least(max(rs.insert_dt), max(in_r.insert_dt), max(d.clndr_dt)) as cf_ooi_inner_update_dt
						from table_rs rs
						inner join table_in_r in_r
							on in_r.rsrc_id = rs.room_id
						left join table_dt d
							on d.clndr_dt between cast(invt_req_strt_dt as date) as cast(invt_req_strt_dt as date)
							and d.clndr_dt between current_date and current_date + 30
						where 1=1
						and in_r.invt_req_vrsn_end_dt = '9999-12-31 00:00:00'
						and in_r.invt_req_sts_nm in ('act')
						group by
						1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16) cf_ooi_inner
					group by
					1,2,3,4,5 ) cf_ooi
						on d.clndr_dt = cf_ooi.calendar_date
						and rg.room_id = cf_ooi.room_id
		left join
			(
				select
					ris.sts_nm
					, rs.rsrc_id
					, rank() over (partition by rs.rsrc_id order by rs.rsrc_sts_vrsn_end_dt) rank_nb
				from table_rs rs
				inner join table_ris
					on rs.rsrc_invt_sts_id = ris_rsrc_invt_sts_id
					and ris_invt_sts_typ = 'yes'
				where 1=1
				and rs.inst_st_nm = 'standard'
				and rs.rsrc_sts_vrsn_strt_dt >= current_date - 10
				) hskp_sts
					on hskp_sts.rsrc_id = rg.room_id
		where 1=1
		and hskp_sts.rank_nb = 1
		group by
		1,2,3,4,5,6,15,16;