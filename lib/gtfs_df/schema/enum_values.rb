# frozen_string_literal: true

module GtfsDf
  module Schema
    module EnumValues
      # trips.txt
      # direction_id: Indicates the direction of travel for a trip.
      DIRECTION_ID = [
        ["0", "Outbound travel"],
        ["1", "Inbound travel"]
      ]

      # wheelchair_accessible: Indicates wheelchair accessibility.
      WHEELCHAIR_ACCESSIBLE = [
        ["0", "No accessibility information"],
        ["1", "Vehicle can accommodate at least one rider in a wheelchair"],
        ["2", "No riders in wheelchairs can be accommodated"]
      ]

      # bikes_allowed: Indicates whether bikes are allowed.
      BIKES_ALLOWED = [
        ["0", "No bike information"],
        ["1", "Vehicle can accommodate at least one bicycle"],
        ["2", "No bicycles allowed"]
      ]

      # cars_allowed: Indicates whether cars are allowed.
      CARS_ALLOWED = [
        ["0", "No car information"],
        ["1", "Vehicle can accommodate at least one car"],
        ["2", "No cars allowed"]
      ]

      # stop_times.txt
      # pickup_type/drop_off_type: Pickup/drop off method.
      PICKUP_TYPE = [
        ["0", "Regularly scheduled pickup"],
        ["1", "No pickup available"],
        ["2", "Must phone agency to arrange pickup"],
        ["3", "Must coordinate with driver to arrange pickup"]
      ]
      DROP_OFF_TYPE = [
        ["0", "Regularly scheduled drop off"],
        ["1", "No drop off available"],
        ["2", "Must phone agency to arrange drop off"],
        ["3", "Must coordinate with driver to arrange drop off"]
      ]

      # continuous_pickup/continuous_drop_off: Continuous stopping behavior.
      CONTINUOUS_PICKUP = [
        ["0", "Continuous stopping pickup"],
        ["1", "No continuous stopping pickup"],
        ["2", "Must phone agency to arrange continuous stopping pickup"],
        ["3", "Must coordinate with driver to arrange continuous stopping pickup"]
      ]
      CONTINUOUS_DROP_OFF = [
        ["0", "Continuous stopping drop off"],
        ["1", "No continuous stopping drop off"],
        ["2", "Must phone agency to arrange continuous stopping drop off"],
        ["3", "Must coordinate with driver to arrange continuous stopping drop off"]
      ]

      # timepoint: Indicates if times are exact or approximate.
      TIMEPOINT = [
        ["0", "Times are approximate"],
        ["1", "Times are exact"]
      ]

      # calendar.txt
      # Service days: 1 = service available, 0 = not available
      SERVICE_DAY = [
        ["0", "Service not available"],
        ["1", "Service available"]
      ]

      # calendar_dates.txt
      # exception_type: Indicates whether service is added or removed for a date.
      EXCEPTION_TYPE = [
        ["1", "Service added for the date"],
        ["2", "Service removed for the date"]
      ]

      # stops.txt
      # location_type: Type of location
      LOCATION_TYPE = [
        ["0", "Stop or platform"],
        ["1", "Station"],
        ["2", "Entrance/Exit"],
        ["3", "Generic Node"],
        ["4", "Boarding Area"]
      ]

      # wheelchair_boarding: Indicates wheelchair boarding possibility
      WHEELCHAIR_BOARDING = [
        ["0", "No accessibility information"],
        ["1", "Some vehicles can be boarded by a rider in a wheelchair"],
        ["2", "Wheelchair boarding not possible"]
      ]

      # stop_access: How the stop is accessed
      STOP_ACCESS = [
        ["0", "Cannot be directly accessed from street network"],
        ["1", "Direct access from street network"]
      ]

      # routes.txt
      # route_type: Type of transportation
      ROUTE_TYPE = [
        ["0", "Tram, Streetcar, Light rail"],
        ["1", "Subway, Metro"],
        ["2", "Rail"],
        ["3", "Bus"],
        ["4", "Ferry"],
        ["5", "Cable tram"],
        ["6", "Aerial lift, suspended cable car"],
        ["7", "Funicular"],
        ["11", "Trolleybus"],
        ["12", "Monorail"]
      ]

      # cemv_support: Contactless EMV support
      CEMV_SUPPORT = [
        ["0", "No cEMV information"],
        ["1", "Riders may use cEMVs as fare media"],
        ["2", "cEMVs are not supported"]
      ]

      # pathways.txt
      # pathway_mode: Type of pathway
      PATHWAY_MODE = [
        ["1", "Walkway"],
        ["2", "Stairs"],
        ["3", "Moving sidewalk/travelator"],
        ["4", "Escalator"],
        ["5", "Elevator"],
        ["6", "Fare gate"],
        ["7", "Exit gate"]
      ]

      # is_bidirectional: Directionality of pathway
      IS_BIDIRECTIONAL = [
        %w[0 Unidirectional],
        %w[1 Bidirectional]
      ]
    end
  end
end
