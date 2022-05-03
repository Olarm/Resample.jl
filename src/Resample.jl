module Resample

using DataFrames, TimesDates, Dates

export resample

function create_grid(start::TimeDate, stop::TimeDate, freq::Minute)
    freq = Dates.Nanosecond(freq.value * 60000000000)
    return create_grid(start, stop, freq)
end


function create_grid(start::TimeDate, stop::TimeDate, freq::Second)
    freq = Dates.Nanosecond(freq.value * 1000000000)
    return create_grid(start, stop, freq)
end


function create_grid(start::TimeDate, stop::TimeDate, freq::Millisecond)
    freq = Dates.Nanosecond(freq.value * 1000000)
    return create_grid(start, stop, freq)
end


function create_grid(start::TimeDate, stop::TimeDate, freq::Microsecond)
    freq = Dates.Nanosecond(freq.value * 1000)
    return create_grid(start, stop, freq)
end


function create_grid(start::TimeDate, stop::TimeDate, freq::Nanosecond)
    dt = stop - start
    K = Int(floor(dt / freq))

    time_grid = Array{DateTime}(undef, K)
    time_grid[1] = DateTime(start)
    for k in 2:K
        time_grid[k] = time_grid[k-1] + freq
    end

    return time_grid
end


function resample(df::DataFrame, start::TimeDate, stop::TimeDate, freq)
    grid_T = create_grid(start, stop, freq)

    gdf = groupby(df, :dataType)
    data_types = [key.dataType for (key, subdf) in pairs(gdf)]

    N = length(grid_T)      # Nr. of rows
    M = length(data_types)  # Nr. of cols
    grid_V = Array{Float64}(undef, N, M)

    for (m, sub_df) in zip(1:M, gdf)
        temp_T = sub_df[!, [:dateTime]].dateTime
        temp_V = sub_df[!, [:value]].value
        for n in 1:N
            if grid_T[n] < temp_T[1]
                grid_V[n,m] = temp_V[1]
            else
                minval, index = findmin(abs.(temp_T .- grid_T[n]))
                if index == 1
                    grid_V[n] = temp_V[index]
                else
                    grid_V[n] = temp_V[index-1]
                end
            end
        end
    end

    resampled_df = DataFrame(grid_V, data_types)
    resampled_df.date_time = grid_T
    return resampled_df
end

end
