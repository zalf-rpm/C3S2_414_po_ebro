def update_parameter_values(ps, params):
    for sampled_name, sampled_value in params.items():
        # separate array index
        parts = sampled_name.rsplit("_", 1)
        if len(parts) == 2 and parts[1].isdigit():
            base_name = parts[0]
            position_in_array = int(parts[1])
        else:
            base_name = sampled_name
            position_in_array = None
        
        # check whether sampled value is a scaling factor
        is_factor = base_name.endswith("Factor")
        pname = base_name.removesuffix("Factor")

        # parameter group (species, cultivar)
        if pname in ps["species"]:
            ptype = "species"
        elif pname in ps["cultivar"]:
            ptype = "cultivar"
        else:
            continue

        # check if the parameter is [data, unit] or [data]
        param_all = ps[ptype][pname]
        has_unit = (isinstance(param_all, list) and len(param_all) == 2 and isinstance(param_all[1], str))
        if has_unit:
            param_val = param_all[0]
        else:
            param_val = param_all

        # explicitly specificed position in array
        if position_in_array is not None:
            if is_factor:
                param_val[position_in_array] *= sampled_value
            else:
                param_val[position_in_array] = sampled_value
        else:
            # default target positions
            indices = None
            if pname == "StageTemperatureSum":
                indices = range(0,6)
            elif pname == "VernalisationRequirement":
                indices = range(0,6)
            elif pname == "BaseDaylength":
                indices = range(2,4)
            elif pname == "DaylengthRequirement":
                indices = range(1,4)
            elif pname == "SpecificLeafArea":
                indices = range(0,6)

            # check if the parameter is an array
            if indices is not None:
                for index in indices:
                    if is_factor:
                        param_val[index] *= sampled_value
                    else:
                        param_val[index] = sampled_value
            else:
                if is_factor:
                    param_val *= sampled_value
                else:
                    param_val = sampled_value

                # param_val is a scalar copy, so assign it back to the original parameter structure
                if has_unit:
                    ps[ptype][pname][0] = param_val
                else:
                    ps[ptype][pname] = param_val

            # additional parameter changes
            if pname == "StageTemperatureSum" and is_factor:
                ps["cultivar"]["BeginSensitivePhaseHeatStress"][0] *= sampled_value
                ps["cultivar"]["EndSensitivePhaseHeatStress"][0] *= sampled_value
    return ps
