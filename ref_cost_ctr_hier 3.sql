
    declare
        cur_row_nbr int;
        loop_count int;
        lvl int;
        prev_row_count int;
        curr_row_count int;
        insert_count int;
        hier varchar(96);
        ctrl_area varchar(48);
        lv varchar(1000);
    begin
        cur_row_nbr := 0;
        loop_count := 1;
        lvl := 1;
        prev_row_count := 0;
        curr_row_count := 0;
        insert_count := 0;
    
       drop table if exists  cost_ctr_hier_unrvl_tmp;
    
	
	CREATE temp TABLE cost_ctr_hier_unrvl_tmp (
    set_class_cd character varying(256) ,
    sub_class_cd character varying(256) ,
    set_nm character varying(256) ,
    set_type_cd character varying(256) ,
    level_nbr bigint ,
    level1_nm character varying(256) ,
    level1_desc character varying(256) ,
    level2_nm character varying(256) ,
    level2_desc character varying(256) ,
    level3_nm character varying(256) ,
    level3_desc character varying(256) ,
    level4_nm character varying(256) ,
    level4_desc character varying(256) ,
    level5_nm character varying(256) ,
    level5_desc character varying(256) ,
    level6_nm character varying(256) ,
    level6_desc character varying(256) ,
    level7_nm character varying(256) ,
    level7_desc character varying(256) ,
    level8_nm character varying(256) ,
    level8_desc character varying(256) ,
    level9_nm character varying(256) ,
    level9_desc character varying(256) ,
    level10_nm character varying(256) ,
    level10_desc character varying(256) ,
    level11_nm character varying(256) ,
    level11_desc character varying(256) ,
    level12_nm character varying(256) ,
    level12_desc character varying(256) ,
    level13_nm character varying(256) ,
    level13_desc character varying(256) ,
    level14_nm character varying(256) ,
    level14_desc character varying(256) ,
    level15_nm character varying(256) ,
    level15_desc character varying(256) ,
    level16_nm character varying(256) ,
    level16_desc character varying(256) ,
    level17_nm character varying(256) ,
    level17_desc character varying(256) ,
    level18_nm character varying(256) ,
    level18_desc character varying(256) ,
    level19_nm character varying(256) ,
    level19_desc character varying(256) ,
    level20_nm character varying(256) ,
    level20_desc character varying(256) 
);
	
    --Delete from sandbox_d4px.cost_ctr_hier_unrvl_tmp where 1=1;
    
    -- Drop and Create table temp hiers from LKP_APPL_HIER
   DROP TABLE IF EXISTS hiers;
    CREATE temp TABLE hiers AS (
        
            SELECT distinct setname, subclass
            from stage.it_user_lkp_appl_hier_lkp_appl_hier_kna  
            WHERE setclass='0101'
        
        --  Add column rownumber and set rownumber  ordered by setname  
        
    );

    
   
    while loop_count > 0 loop -- loop to iterate while data in Hiers
        -- take min row number and remove from hiers table at end till reaches 0
        --select min(row_nbr) into cur_row_nbr from hiers;
        --raise info 'working for row_nbr = %', cur_row_nbr;
        select min(setname) into hier from hiers;
   select min(subclass) into ctrl_area from hiers where setname = hier;
  lvl := 1;
        select count(*) into prev_row_count from cost_ctr_hier_unrvl_tmp;

        -- Insert data to level 1
        insert into cost_ctr_hier_unrvl_tmp(
            set_class_cd, sub_class_cd, 
            set_nm, set_type_cd, level_nbr, 
            level1_nm, level1_desc)
            SELECT
                h.SETCLASS,
                h.SUBCLASS,
                h.SETNAME,
                h.SETTYPE,
                1,
                h.SETNAME,
                t.DESCRIPT
            FROM
                stage.kna_ecc_fin_hier_setheader h,
                stage.kna_ecc_fin_hier_setheadert t
            WHERE
                h.SETCLASS = '0101'
                and h.SUBCLASS = ctrl_area
                and h.SETNAME = hier
                and h.SETCLASS = t.SETCLASS
                and h.SUBCLASS = t.SUBCLASS
                and h.SETNAME = t.SETNAME
                and t.LANGU = 'E';

        select count(*) into curr_row_count from cost_ctr_hier_unrvl_tmp;
      --insert into public.query_list values ( curr_row_count );
        
       --insert into public.query_list values ( hier );
        

        insert_count := curr_row_count - prev_row_count;
       --insert into public.query_list values ( insert_count );
        
        while insert_count > 0 loop -- loop to add nodes till last node
            lvl := lvl + 1;
            if lvl > 20 then 
                raise exception 'LEVELS exceeded';
            end if;
           --lv = lvl || '-' || hier;
          --insert into public.query_list values (lv);

            -- set prev_row_count to current row count in table
            prev_row_count := curr_row_count;

            --insert data of next node in table
            insert into cost_ctr_hier_unrvl_tmp(
                SET_CLASS_cd ,SUB_CLASS_cd ,SET_nm ,SET_TYPE_cd
                ,level_nbr ,level1_nm  ,level1_desc ,level2_nm ,level2_desc 
                ,level3_nm  ,level3_desc ,level4_nm  ,level4_desc 
                ,level5_nm  ,level5_desc ,level6_nm  ,level6_desc 
                ,level7_nm  ,level7_desc ,level8_nm  ,level8_desc 
                ,level9_nm  ,level9_desc ,level10_nm  ,level10_desc 
                ,level11_nm  ,level11_desc ,level12_nm  ,level12_desc 
                ,level13_nm  ,level13_desc ,level14_nm  ,level14_desc 
                ,level15_nm  ,level15_desc ,level16_nm  ,level16_desc 
                ,level17_nm  ,level17_desc ,level18_nm  ,level18_desc 
                ,level19_nm  ,level19_desc ,level20_nm  ,level20_desc
            )
            select 
                h.SETCLASS,
                h.SUBCLASS,
                h.SETNAME,
                h.SETTYPE,
                lvl,
                z.level1_nm,
                z.level1_desc,
                case
                    when lvl = 2 then h.SETNAME
                    else z.level2_nm
                end,
                case
                    when lvl = 2 then t.DESCRIPT
                    else z.level2_desc
                end,
                case
                    when lvl = 3 then h.SETNAME
                    else z.level3_nm
                end,
                case
                    when lvl = 3 then t.DESCRIPT
                    else z.level3_desc
                end,
                case
                    when lvl = 4 then h.SETNAME
                    else z.level4_nm
                end,
                case
                    when lvl = 4 then t.DESCRIPT
                    else z.level4_desc
                end,
                case
                    when lvl = 5 then h.SETNAME
                    else z.level5_nm
                end,
                case
                    when lvl = 5 then t.DESCRIPT
                    else z.level5_desc
                end,
                case
                    when lvl = 6 then h.SETNAME
                    else z.level6_nm
                end,
                case
                    when lvl = 6 then t.DESCRIPT
                    else z.level6_desc
                end,
                case
                    when lvl = 7 then h.SETNAME
                    else z.level7_nm
                end,
                case
                    when lvl = 7 then t.DESCRIPT
                    else z.level7_desc
                end,
                case
                    when lvl = 8 then h.SETNAME
                    else z.level8_nm
                end,
                case
                    when lvl = 8 then t.DESCRIPT
                    else z.level8_desc
                end,
                case
                    when lvl = 9 then h.SETNAME
                    else z.level9_nm
                end,
                case
                    when lvl = 9 then t.DESCRIPT
                    else z.level9_desc
                end,
                case
                    when lvl = 10 then h.SETNAME
                    else z.level10_nm
                end,
                case
                    when lvl = 10 then t.DESCRIPT
                    else z.level10_desc
                end,
                case
                    when lvl = 11 then h.SETNAME
                    else z.level11_nm
                end,
                case
                    when lvl = 11 then t.DESCRIPT
                    else z.level11_desc
                end,
                case
                    when lvl = 12 then h.SETNAME
                    else z.level12_nm
                end,
                case
                    when lvl = 12 then t.DESCRIPT
                    else z.level12_desc
                end,
                case
                    when lvl = 13 then h.SETNAME
                    else z.level13_nm
                end,
                case
                    when lvl = 13 then t.DESCRIPT
                    else z.level13_desc
                end,
                case
                    when lvl = 14 then h.SETNAME
                    else z.level14_nm
                end,
                case
                    when lvl = 14 then t.DESCRIPT
                    else z.level14_desc
                end,
                case
                    when lvl = 15 then h.SETNAME
                    else z.level15_nm
                end,
                case
                    when lvl = 15 then t.DESCRIPT
                    else z.level15_desc
                end,
                case
                    when lvl = 16 then h.SETNAME
                    else z.level16_nm
                end,
                case
                    when lvl = 16 then t.DESCRIPT
                    else z.level16_desc
                end,
                case
                    when lvl = 17 then h.SETNAME
                    else z.level17_nm
                end,
                case
                    when lvl = 17 then t.DESCRIPT
                    else z.level17_desc
                end,
                case
                    when lvl = 18 then h.SETNAME
                    else z.level18_nm
                end,
                case
                    when lvl = 18 then t.DESCRIPT
                     else z.level18_desc
                end,
                case
                    when lvl = 19 then h.SETNAME
                    else z.level19_nm
                end,
                case
                    when lvl = 19 then t.DESCRIPT
                    else z.level19_desc
                end,
                case
                    when lvl = 20 then h.SETNAME
                    else z.level20_nm
                end,
                case
                    when lvl = 20 then t.DESCRIPT
                    else z.level20_desc
                end
            from stage.kna_ecc_fin_hier_setheader h,
                stage.kna_ecc_fin_hier_setheadert t,
                stage.kna_ecc_fin_hier_setnode n,
                cost_ctr_hier_unrvl_tmp z
            where h.SETCLASS = n.SUBSETCLS
                and h.SUBCLASS = n.SUBSETSCLS
                and h.SETNAME = n.SUBSETNAME
                and h.SETCLASS = t.SETCLASS
                and h.SUBCLASS = t.SUBCLASS
                and h.SETNAME = t.SETNAME
                and t.LANGU = 'E'
                and n.SETCLASS = z.set_class_cd
                and n.SUBCLASS = z.sub_class_cd
                and n.SETNAME = z.set_nm
                and z.level1_nm = hier
                and z.SET_TYPE_cd <> 'B'
                and (n.SUBSETCLS + n.SUBSETSCLS + n.SUBSETNAME) not in (
                    select
                        SET_CLASS_cd + SUB_CLASS_cd + SET_nm
                    from
                       cost_ctr_hier_unrvl_tmp z1
                    WHERE
                        level1_nm = hier
                )
                and z.LeVeL_NBR = (lvl - 1);

            
            select count(*) into curr_row_count from cost_ctr_hier_unrvl_tmp;
            -- insert count will be zero if there are no new lvl hence loop will end   
            insert_count := curr_row_count - prev_row_count;
           
          -- insert into public.query_list  values (insert_count);

        end loop;

        --Inert cost_ctr from csks & cskt to the next level of each leafnode
        insert into cost_ctr_hier_unrvl_tmp(
                SET_CLASS_cd ,SUB_CLASS_cd ,SET_nm ,SET_TYPE_cd
                ,level_nbr ,level1_nm  ,level1_desc ,level2_nm ,level2_desc 
                ,level3_nm  ,level3_desc ,level4_nm  ,level4_desc 
                ,level5_nm  ,level5_desc ,level6_nm  ,level6_desc 
                ,level7_nm  ,level7_desc ,level8_nm  ,level8_desc 
                ,level9_nm  ,level9_desc ,level10_nm  ,level10_desc 
                ,level11_nm  ,level11_desc ,level12_nm  ,level12_desc 
                ,level13_nm  ,level13_desc ,level14_nm  ,level14_desc 
                ,level15_nm  ,level15_desc ,level16_nm  ,level16_desc 
                ,level17_nm  ,level17_desc ,level18_nm  ,level18_desc 
                ,level19_nm  ,level19_desc ,level20_nm  ,level20_desc
            )
            select 
                z.set_class_cd,
                z.sub_class_cd,
                c.KOSTL,
                'X'::text,
                level_nbr + 1,
                z.level1_nm,
                z.level1_desc,
                case
                    when level_nbr + 1 = 2 then c.KOSTL
                    else z.level2_nm
                end,
                case
                    when level_nbr + 1 = 2 then t.KTEXT
                    else z.level2_desc
                end,
                case
                    when level_nbr + 1= 3 then c.KOSTL
                    else z.level3_nm
                end,
                case
                    when level_nbr + 1= 3 then t.KTEXT
                    else z.level3_desc
                end,
                case
                    when level_nbr + 1= 4 then c.KOSTL
                    else z.level4_nm
                end,
                case
                    when level_nbr + 1= 4 then t.KTEXT
                    else z.level4_desc
                end,
                case
                    when level_nbr + 1= 5 then c.KOSTL
                    else z.level5_nm
                end,
                case
                    when level_nbr + 1= 5 then t.KTEXT
                    else z.level5_desc
                end,
                case
                    when level_nbr + 1= 6 then c.KOSTL
                    else z.level6_nm
                end,
                case
                    when level_nbr + 1= 6 then t.KTEXT
                    else z.level6_desc
                end,
                case
                    when level_nbr + 1= 7 then c.KOSTL
                    else z.level7_nm
                end,
                case
                    when level_nbr + 1= 7 then t.KTEXT
                    else z.level7_desc
                end,
                case
                    when level_nbr + 1= 8 then c.KOSTL
                    else z.level8_nm
                end,
                case
                    when level_nbr + 1= 8 then t.KTEXT
                    else z.level8_desc
                end,
                case
                    when level_nbr + 1= 9 then c.KOSTL
                    else z.level9_nm
                end,
                case
                    when level_nbr + 1= 9 then t.KTEXT
                    else z.level9_desc
                end,
                case
                    when level_nbr + 1= 10 then c.KOSTL
                    else z.level10_nm
                end,
                case
                    when level_nbr + 1= 10 then t.KTEXT
                    else z.level10_desc
                end,
                case
                    when level_nbr + 1= 11 then c.KOSTL
                    else z.level11_nm
                end,
                case
                    when level_nbr + 1= 11 then t.KTEXT
                    else z.level11_desc
                end,
                case
                    when level_nbr + 1= 12 then c.KOSTL
                    else z.level12_nm
                end,
                case
                    when level_nbr + 1= 12 then t.KTEXT
                    else z.level12_desc
                end,
                case
                    when level_nbr + 1= 13 then c.KOSTL
                    else z.level13_nm
                end,
                case
                    when level_nbr + 1= 13 then t.KTEXT
                    else z.level13_desc
                end,
                case
                    when level_nbr + 1= 14 then c.KOSTL
                    else z.level14_nm
                end,
                case
                    when level_nbr + 1= 14 then t.KTEXT
                    else z.level14_desc
                end,
                case
                    when level_nbr + 1= 15 then c.KOSTL
                    else z.level15_nm
                end,
                case
                    when level_nbr + 1= 15 then t.KTEXT
                    else z.level15_desc
                end,
                case
                    when level_nbr + 1= 16 then c.KOSTL
                    else z.level16_nm
                end,
                case
                    when level_nbr + 1= 16 then t.KTEXT
                    else z.level16_desc
                end,
                case
                    when level_nbr + 1= 17 then c.KOSTL
                    else z.level17_nm
                end,
                case
                    when level_nbr + 1= 17 then t.KTEXT
                    else z.level17_desc
                end,
                case
                    when level_nbr + 1= 18 then c.KOSTL
                    else z.level18_nm
                end,
                case
                    when level_nbr + 1= 18 then t.KTEXT
                    else z.level18_desc
                end,
                case
                    when level_nbr + 1= 19 then c.KOSTL
                    else z.level19_nm
                end,
                case
                    when level_nbr + 1= 19 then t.KTEXT
                    else z.level19_desc
                end,
                case
                    when level_nbr + 1= 20 then c.KOSTL
                    else z.level20_nm
                end,
                case
                    when level_nbr + 1= 20 then t.KTEXT
                    else z.level20_desc
                end
            from 
                stage.kna_ecc_cost_ctr_mstr_csks c,
                stage.kna_ecc_cost_ctr_mstr_cskt t,
                stage.kna_ecc_fin_hier_setleaf l,
                cost_ctr_hier_unrvl_tmp z
            where (
                    z.SET_TYPE_cd = 'B'
                    or z.SET_TYPE_cd = 'S'
                )
                and c.KOKRS = ctrl_area
                and c.DATAB <= getdate()
                and c.DATBI >= getdate()
                and c.KOKRS = t.KOKRS
                and c.KOSTL = t.KOSTL
                and t.SPRAS = 'E'
                and c.DATBI = t.DATBI
                and z.level1_nm = hier
                and z.SET_CLASS_cd = l.SETCLASS
                and z.SUB_CLASS_cd = l.SUBCLASS
                and z.SET_nm = l.SETNAME
                and (
                    l.VALSIGN = 'I'
                    and (
                        (
                            l.VALOPTION = 'EQ'
                            and c.KOSTL = l.VALFROM
                        )
                        or (
                            l.VALOPTION = 'BT'
                            and c.KOSTL >= l.VALFROM
                            and c.KOSTL <= l.VALTO
                        )
                    )
                );
        
        -- removing cur_row_nbr row from hiers  
        delete from hiers where setname = hier;
        -- sent loop_count to count from hiers
        select count(*) into loop_count from hiers ;
    END LOOP;

    --Empty table before inserting latest data 
    Delete from fin_acctg_ops.ref_cost_ctr_hier where 1=1;

    --Insert into cost_ctr_hier_unrvl (Finaml Output) from  LKP_APPL_HIER and cost_ctr_hier_unrvl_tmp
    insert into fin_acctg_ops.ref_cost_ctr_hier
    select c.APPL_NM, h.*, c.QUALFR, c.src_nm , md5(c.src_nm)  , c.kortex_dprct_ts  ,c.kortex_upld_ts ,
    case 
	    when level_nbr = 1 then level1_nm
	    when level_nbr = 2 then level2_nm
	    when level_nbr = 3 then level3_nm
	    when level_nbr = 4 then level4_nm
	    when level_nbr = 5 then level5_nm
	    when level_nbr = 6 then level6_nm
	    when level_nbr = 7 then level7_nm
	    when level_nbr = 8 then level8_nm
	    when level_nbr = 9 then level9_nm
	    when level_nbr = 10 then level10_nm
	    when level_nbr = 11 then level11_nm
	    when level_nbr = 12 then level12_nm
	    when level_nbr = 13 then level13_nm
	    when level_nbr = 14 then level14_nm
	    when level_nbr = 15 then level15_nm
	    when level_nbr = 16 then level16_nm
	    when level_nbr = 17 then level17_nm
	    when level_nbr = 18 then level18_nm
	    when level_nbr = 19 then level19_nm
	    when level_nbr = 20 then level20_nm
	    end as last_level_nm,
	    case 
	    when level_nbr = 1 then level1_desc
	    when level_nbr = 2 then level2_desc
	    when level_nbr = 3 then level3_desc
	    when level_nbr = 4 then level4_desc
	    when level_nbr = 5 then level5_desc
	    when level_nbr = 6 then level6_desc
	    when level_nbr = 7 then level7_desc
	    when level_nbr = 8 then level8_desc
	    when level_nbr = 9 then level9_desc
	    when level_nbr = 10 then level10_desc
	    when level_nbr = 11 then level11_desc
	    when level_nbr = 12 then level12_desc
	    when level_nbr = 13 then level13_desc
	    when level_nbr = 14 then level14_desc
	    when level_nbr = 15 then level15_desc
	    when level_nbr = 16 then level16_desc
	    when level_nbr = 17 then level17_desc
	    when level_nbr = 18 then level18_desc
	    when level_nbr = 19 then level19_desc
	    when level_nbr = 20 then level20_desc
	    end as last_level_desc

    from  cost_ctr_hier_unrvl_tmp h,
        stage.it_user_lkp_appl_hier_lkp_appl_hier_kna c
    where h.SET_CLASS_cd=c.SETCLASS
        and h.SUB_CLASS_cd=c.SUBCLASS
        and h.level1_nm=c.SETNAME;
       
       
    
    --raise exception if any
    exception when others then 
        Raise exception 'Stored Procedure Failed - Rollback initiated';
    end;

