CREATE 
    ALGORITHM = UNDEFINED 
    DEFINER = `root`@`localhost` 
    SQL SECURITY DEFINER
VIEW `sapn_grid`.`v_sa1_analysis` AS
    SELECT 
        `sapn_grid`.`v_sa1_base`.`timestamp` AS `timestamp`,
        `sapn_grid`.`v_sa1_base`.`wind_mw` AS `wind_mw`,
        `sapn_grid`.`v_sa1_base`.`solar_utility_mw` AS `solar_utility_mw`,
        `sapn_grid`.`v_sa1_base`.`solar_rooftop_mw` AS `solar_rooftop_mw`,
        `sapn_grid`.`v_sa1_base`.`battery_charging_mw` AS `battery_charging_mw`,
        `sapn_grid`.`v_sa1_base`.`battery_discharging_mw` AS `battery_discharging_mw`,
        `sapn_grid`.`v_sa1_base`.`gas_ccgt_mw` AS `gas_ccgt_mw`,
        `sapn_grid`.`v_sa1_base`.`gas_ocgt_mw` AS `gas_ocgt_mw`,
        `sapn_grid`.`v_sa1_base`.`gas_steam_mw` AS `gas_steam_mw`,
        `sapn_grid`.`v_sa1_base`.`gas_reciprocating_mw` AS `gas_reciprocating_mw`,
        `sapn_grid`.`v_sa1_base`.`distillate_mw` AS `distillate_mw`,
        `sapn_grid`.`v_sa1_base`.`imports_mw` AS `imports_mw`,
        `sapn_grid`.`v_sa1_base`.`exports_mw` AS `exports_mw`,
        `sapn_grid`.`v_sa1_base`.`wind_curtailment_mw` AS `wind_curtailment_mw`,
        `sapn_grid`.`v_sa1_base`.`solar_curtailment_mw` AS `solar_curtailment_mw`,
        `sapn_grid`.`v_sa1_base`.`temperature_c` AS `temperature_c`,
        `sapn_grid`.`v_sa1_base`.`price` AS `price`,
        `sapn_grid`.`v_sa1_base`.`emissions_intensity` AS `emissions_intensity`,
        ((`sapn_grid`.`v_sa1_base`.`wind_mw` + `sapn_grid`.`v_sa1_base`.`solar_utility_mw`) + `sapn_grid`.`v_sa1_base`.`solar_rooftop_mw`) AS `total_renewables_mw`,
        (((`sapn_grid`.`v_sa1_base`.`gas_ccgt_mw` + `sapn_grid`.`v_sa1_base`.`gas_ocgt_mw`) + `sapn_grid`.`v_sa1_base`.`gas_steam_mw`) + `sapn_grid`.`v_sa1_base`.`gas_reciprocating_mw`) AS `total_gas_mw`,
        ((((((((`sapn_grid`.`v_sa1_base`.`wind_mw` + `sapn_grid`.`v_sa1_base`.`solar_utility_mw`) + `sapn_grid`.`v_sa1_base`.`solar_rooftop_mw`) + `sapn_grid`.`v_sa1_base`.`gas_ccgt_mw`) + `sapn_grid`.`v_sa1_base`.`gas_ocgt_mw`) + `sapn_grid`.`v_sa1_base`.`gas_steam_mw`) + `sapn_grid`.`v_sa1_base`.`gas_reciprocating_mw`) + `sapn_grid`.`v_sa1_base`.`battery_discharging_mw`) + `sapn_grid`.`v_sa1_base`.`distillate_mw`) AS `total_generation_mw`,
        ROUND(((((`sapn_grid`.`v_sa1_base`.`wind_mw` + `sapn_grid`.`v_sa1_base`.`solar_utility_mw`) + `sapn_grid`.`v_sa1_base`.`solar_rooftop_mw`) / NULLIF(((((((((`sapn_grid`.`v_sa1_base`.`wind_mw` + `sapn_grid`.`v_sa1_base`.`solar_utility_mw`) + `sapn_grid`.`v_sa1_base`.`solar_rooftop_mw`) + `sapn_grid`.`v_sa1_base`.`gas_ccgt_mw`) + `sapn_grid`.`v_sa1_base`.`gas_ocgt_mw`) + `sapn_grid`.`v_sa1_base`.`gas_steam_mw`) + `sapn_grid`.`v_sa1_base`.`gas_reciprocating_mw`) + `sapn_grid`.`v_sa1_base`.`battery_discharging_mw`) + `sapn_grid`.`v_sa1_base`.`distillate_mw`),
                        0)) * 100),
                2) AS `renewable_pct`,
        (`sapn_grid`.`v_sa1_base`.`wind_curtailment_mw` + `sapn_grid`.`v_sa1_base`.`solar_curtailment_mw`) AS `total_curtailment_mw`,
        (CASE
            WHEN (`sapn_grid`.`v_sa1_base`.`price` < 0) THEN 1
            ELSE 0
        END) AS `is_negative_price`,
        MINUTE(`sapn_grid`.`v_sa1_base`.`timestamp`) AS `minute_of_day`,
        HOUR(`sapn_grid`.`v_sa1_base`.`timestamp`) AS `hour_of_day`,
        DAYNAME(`sapn_grid`.`v_sa1_base`.`timestamp`) AS `day_name`,
        CAST(`sapn_grid`.`v_sa1_base`.`timestamp` AS DATE) AS `date_only`
    FROM
        `sapn_grid`.`v_sa1_base`