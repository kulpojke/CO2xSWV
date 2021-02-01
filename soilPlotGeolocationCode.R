# Code to calculate sensor geolocation within soil plot
# Author: Ed Ayres
# Date: 2018-08-08

setwd("N:/Science/FIU/Soil Array/Soil Sensor Geolocation/Plots/")
library(jsonlite)
library(stringr)
library(prettymapr)
library(rgdal)

# Read in plot urls
plot_urls <- read.csv("N:/Science/FIU/Soil Array/Soil Sensor Geolocation/plot_links.csv")
SP_geoloc <- as.character(plot_urls[1:nrow(plot_urls),2])
for(k in 3:ncol(plot_urls)){
  SP_geoloc <- append(SP_geoloc, as.character(plot_urls[1:nrow(plot_urls),k]))
}

# Read in approximate site elevation file (based on tower location)
siteElev <- read.csv("N:/Science/FIU/Soil Array/Soil Sensor Geolocation/siteElevationsApprox.csv")

# Create a vector for bad urls (if any) to be stored. Some soil plot geolocation files seem to be missing utm geolocations.
urlsMissingUTMdata <- "Soil plot geolocation data does not include utm"
# Create vector for positive or 0 soil sensor depths, or depths that appear to be in the wrong units (i.e., cm not m)
badSensorDepths <- "Sensor depth is outside expected range (Temp <0 to -2 m, CO2 <0 to -0.5 m, Heat flux -0.06 to -0.1, NR 0.2 to 0.3 m, IR 0.1 to 0.5 m"
badSensorDepths_depth <- "CFGLOC_depth"
badSensorDepths_site <- "CFGLOC_site"
badSensorDepths_sensor <- "CFGLOC_sensor"

# Create vector listing locations where one or more utm coordinate appear to be incorrect
badUTMvalues <- "UTM (and possibly lat/long) for one or more soil plot corners appear to be incorrect (i.e., max-min UTM northing or UTM easting > 10 meters)"
# Create vector listing locations for sensors that may not actually exist in the field
nonexistentSensors <- "Potentially not actually present in soil plot, or if present, possibly with incorrect offset values"
# Create vector for soil plots that may be incorrectly oriented (i.e., radiation section of soil plot is not on the southern side)
possibleBadOrientation <- "Possibly incorrectly oriented soil plot (i.e., radiation section of soil plot is not on the southern side)"
# Create vector for soil plots with bad elevation data (possible in ft units)
badPlotElevation <- "One or more soil plot corners have an elevation >50 m different than approx tower elevation (possibly ft units, not m)"
badPlotElevation_site <- "CFGLOC_site"
badPlotElevation_minElev <- "CFGLOC_minElev"
badPlotElevation_maxElev <- "CFGLOC_maxElev"
badPlotElevation_towerElev <- "Approx tower elev (m)"
# Create vector for bad xOffset
badxOffset <- "xOffset not between -1 m and 8 m (-3 m and 10 m for relative humidity). Possibly incorrect units"
# Create vector for bad xOffset
badyOffset <- "yOffset not between -1 m and 8 m (-3 m and 10 m for relative humidity). Possibly incorrect units"
# Create vector for bad alpha orientation
badAlpha <- "Implausible alpha orientation for this sensor type"
# Create vector for bad beta orientation
badBeta <- "Implausible beta orientation for this sensor type"
# Create vector for bad gamma orientation
badGamma <- "Implausible gamma orientation for this sensor type"

# read in soil plot json object
for(j in 1:length(SP_geoloc)){
  tryCatch({
    plot_geo <- fromJSON(SP_geoloc[j])
    # plot_geo <- fromJSON("http://data.neonscience.org/api/v0/locations/SOILPL100213")
    
    # Get soil plot corner eastings and northings (UTM). 
    # Corners are ordered: C1, C4, C3, C2, C1. C1 represents the reference corner. C1 to C4 is the vector along which xOffset in the sensor geolocation file is measured. C1 to C2 is the vector along which yOffset in the sensor geolocation file is measured.
    utm <- data.frame(plot_geo$data$locationPolygon$coordinates$utmEasting)
    utm$northing <- plot_geo$data$locationPolygon$coordinates$utmNorthing
    
    # Record url if utm geolocation data are missing
    if(ncol(utm) == 0){
      urlsMissingUTMdata <- append(urlsMissingUTMdata, SP_geoloc[j])}
    
    # Add column and row names
    colnames(utm) <- c("easting", "northing")
    row.names(utm) <- c("C1", "C4", "C3", "C2", "C1.repeat")
    
    # Check whether corner C1 is on the southern side of the plot. C1 is the corner that is usually in the radiation section of the plot, which is supposed to be on the south side.
    if((utm["C1", "northing"] - 1) > utm["C2", "northing"] | (utm["C1", "northing"] - 1) > utm["C4", "northing"] ){
      possibleBadOrientation <- append(possibleBadOrientation, SP_geoloc[j])
    }
    
    # Check whether all soil plot corner elevations are within 50 m of approx tower elevation at that site
    elevThreshold <- siteElev$elevation_m[which(siteElev$site == plot_geo$data$siteCode)]
    if(min(plot_geo$data$locationPolygon$coordinates$elevation) < (elevThreshold - 50) | max(plot_geo$data$locationPolygon$coordinates$elevation) > (elevThreshold + 50)){
      badPlotElevation <- append(badPlotElevation, SP_geoloc[j])
      badPlotElevation_site <- append(badPlotElevation_site, plot_geo$data$siteCode)
      badPlotElevation_minElev <- append(badPlotElevation_minElev, min(plot_geo$data$locationPolygon$coordinates$elevation))
      badPlotElevation_maxElev <- append(badPlotElevation_maxElev, max(plot_geo$data$locationPolygon$coordinates$elevation))
      badPlotElevation_towerElev <- append(badPlotElevation_towerElev, elevThreshold)
    }
    # Reset elevation threshold to prevent carryover to next soil plot
    elevThreshold <- NA
    
    # Calculate mean and sd elevation
    mean_plot_elevation <- mean(plot_geo$data$locationPolygon$coordinates$elevation)
    sd_plot_elevation <- sd(plot_geo$data$locationPolygon$coordinates$elevation)
    # Make a name for a graph of the soil plot and sensor
    geolocDesc <- plot_geo$data$locationDescription
    plotID <- substr(geolocDesc, nchar(geolocDesc)-2, nchar(geolocDesc))
    graph_name <- paste0(plot_geo$data$domainCode, " ", plot_geo$data$siteCode, " ", plotID, " ", plot_geo$data$locationName)
    
    # Graph soil plot corners. Soil plot may not be perfectly square dues to uncertainty in surveyed corner locations.
    y_min <- min(utm$northing-1.75)
    y_max <- max(utm$northing+1.75)
    x_min <- min(utm$easting-1.75)
    x_max <- max(utm$easting+1.75)
    
    # Record url if utm geolocation data seem suspicious (i.e., soil plot side > 10 m long)
    if((y_max-y_min) > (10+1.25+1.25) | (x_max-x_min)>(10+1.25+1.25)){
      badUTMvalues <- append(badUTMvalues, SP_geoloc[j])}
    
    # Make a graph of the soil plot boundary
    filename1 <- paste0(graph_name, ".png")
    png(filename1, width=800, height=800, units="px", res=150)
    plot(utm$easting, utm$northing, type="l", xlab="Easting (m)", ylab="Northing (m)", xlim <- c(x_min, x_max), ylim <- c(y_min, y_max), main = graph_name, col="grey")
    with(utm, text(utm, labels = c("C1", "C4", "C3", "C2"), col="grey", cex=0.5, pos = 4))
    
    # Calculate the distance from C1 to C4 and C1 to C2 based on pythagorean theorem (m)
    dist_C14 <- sqrt((utm["C4", "easting"] - utm["C1", "easting"])^2 + (utm["C4", "northing"] - utm["C1", "northing"])^2 )
    dist_C12 <- sqrt((utm["C2", "easting"] - utm["C1", "easting"])^2 + (utm["C2", "northing"] - utm["C1", "northing"])^2 )
    
    # Calculate gradient of C1 to C4 line and C1 to C2 line
    grad_C14 <- (utm["C4", "northing"] - utm["C1", "northing"]) / (utm["C4", "easting"] - utm["C1", "easting"])
    grad_C12 <- (utm["C2", "northing"] - utm["C1", "northing"]) / (utm["C2", "easting"] - utm["C1", "easting"])
    
    # Calculate the gradient of a line perpendicular to C1 to C4 and a line perpendicular to C1 to C2
    grad_perpen_C14<- 1/(grad_C14 * -1)
    grad_perpen_C12<- 1/(grad_C12 * -1)
    
    
    ### ADD SENSOR LOCATIONS TO PLOT 
    # Get url for all child (i.e., sensor) geolocation data
    sensor_geolocs <- plot_geo$data$locationChildrenUrls
    
        # Function to draw elipses to represent error bars
    draw_ellipse = function (mean_x, mean_y, sd_x, sd_y)
    {
      ellipse <- function (x) { sin(acos(x)) }
      t = seq(-1, 1, length.out = 100)
      el_y = sd_y*ellipse(t)
      newx = mean_x + sd_x * t
      polygon(c(newx, rev(newx)), c(mean_y + el_y, rev(mean_y - el_y)), col = rgb(1, 0, 0, 0.1), border = NA)
    }
    
    # Create vector for sensor name, and also for temp and co2 sensor depths
    sensorName <- c()
    tempSensorName <- c()
    tempSensorDepths <- c()
    co2SensorName <- c()
    co2SensorDepths <- c()
    
    # Create vector for sensor depth threshold
    T_depthThreshold <- c()
    
    # Create data frame to store sensor geolocation data
    sensorGeo <- data.frame(matrix(ncol = 18, nrow = 0), stringsAsFactors = F)
    colnames(sensorGeo) <- c("sensorName", "height_sensor", "alpha_sensor", "beta_sensor", "gamma_sensor", "mean_easting_sensor", "sd_easting_sensor", "mean_northing_sensor", "sd_northing_sensor", "utm_hemisphere_sensor", "utm_zone_sensor", "mean_plot_elevation", "sd_plot_elevation", "domain", "site", "soilPlot", "plotLocationName", "sensorLocationName")
    
    # Read in the child geolocation data and get the x and y offset values
    for(i in 1:length(sensor_geolocs)){
      child_geo <- fromJSON(sensor_geolocs[i])
      #child_geo <- fromJSON("http://data.neonscience.org/api/v0/locations/CFGLOC105017")
      
      # Skip this location (CFGLOC) if it is not used. Do this step here and just below in case this location isn't a CFGLOC or a parent
      if(!grepl(pattern = "NOT USED", x=toupper(child_geo$data$locationDescription))){
        # If the location name is not an actual sensor (i.e., does not start "CFG"), but instead a parent to a sensor, then load the child sensor data.
        if(!grepl(pattern = "CFG", x=child_geo$data$locationName)){
          if(!grepl(pattern = "CFG", x=child_geo$data$locationName)){
            child_geo <- fromJSON(child_geo$data$locationChildrenUrls)
          }
        }
      }
      
      # Skip this location (CFGLOC) if it is not used
      if(!grepl(pattern = "NOT USED", x=toupper(child_geo$data$locationDescription))){

        # Grab the x and y offsets of the sensor (i.e., distance from reference corner along soil plot sides)
        dist_intersect_C14 <- child_geo$data$xOffset
        dist_intersect_C12 <- child_geo$data$yOffset
        
        # Get urls for potentially non-existent sensors (i.e., where x, y, and z offsets are all zero)
        if(child_geo$data$xOffset == 0 && child_geo$data$yOffset == 0 && child_geo$data$zOffset == 0){
          nonexistentSensors <- append(nonexistentSensors, child_geo$data$locationName)}
  
        # Get sensor name and check that sensor position data are plausible
        if(grepl("Soil Temp", child_geo$data$locationDescription)){
          sensorName <- paste0("T_", substr(child_geo$data$locationDescription, str_locate(child_geo$data$locationDescription, " Z")[2], str_locate(child_geo$data$locationDescription, " Z")[2] + 1))
          tempSensorName <- append(tempSensorName, sensorName)
          tempSensorDepths <- append(tempSensorDepths, child_geo$data$zOffset)
          if(child_geo$data$domainCode == "D18" | child_geo$data$domainCode == "D19"){
            T_depthThreshold <- -3
          }else{
            T_depthThreshold <- -2
          }
          # Check if depth is plausible (0 to -2 m)
          if(child_geo$data$zOffset > 0 | child_geo$data$zOffset < T_depthThreshold){
            # Don't add sensor to "badSensorDepths" if it's already in "nonexistentSensors"
            if(sum(grepl(pattern = child_geo$data$locationName, x = nonexistentSensors)) == 0){
              badSensorDepths <- append(badSensorDepths, child_geo$data$locationName)
              badSensorDepths_depth <- append(badSensorDepths_depth, child_geo$data$zOffset)
              badSensorDepths_site <- append(badSensorDepths_site, child_geo$data$siteCode)
              badSensorDepths_sensor <- append(badSensorDepths_sensor, sensorName)
            }
          }
          # Check if xOffset is plausible (-1 m to 8 m)
          if(child_geo$data$xOffset < -1 | child_geo$data$xOffset > 8){
            badxOffset <- append(badxOffset, child_geo$data$locationName)
          }
          # Check if yOffset is plausible (-1 m to 8 m)
          if(child_geo$data$yOffset < -1 | child_geo$data$yOffset > 8){
            badyOffset <- append(badyOffset, child_geo$data$locationName)
          }
          # Check if sensor angle (degrees from vertical) is plausible. Usually will be less than 3 degrees, so test threshold set to 5 degrees.
          if(grepl("Soil Temp and Moisture", child_geo$data$locationDescription)){
            if(child_geo$data$alphaOrientation > 45){ # combined temp and moisture sensor is installed parallel to soil surface (currently only at PUUM)
              badAlpha <- append(badAlpha, child_geo$data$locationName)
            }
          }else if(child_geo$data$alphaOrientation > 5){
            badAlpha <- append(badAlpha, child_geo$data$locationName)
          }
          # Check if beta orientation is >0. Should not be as this parameter is not used for this sensor.
          if(child_geo$data$betaOrientation != 0){
            badBeta <- append(badBeta, child_geo$data$locationName)
          }
          # Check if azimuth is plausible (i.e., 0-360 degrees)
          if(child_geo$data$gammaOrientation < 0 | child_geo$data$gammaOrientation > 360){
            badGamma <- append(badGamma, child_geo$data$locationName)
          }
          
          
        }else if(grepl("PAR", child_geo$data$locationDescription)){
          sensorName <- "PAR"
          # Check if xOffset is plausible (-1 m to 8 m)
          if(child_geo$data$xOffset < -1 | child_geo$data$xOffset > 8){
            badxOffset <- append(badxOffset, child_geo$data$locationName)
          }
          # Check if yOffset is plausible (-1 m to 8 m)
          if(child_geo$data$yOffset < -1 | child_geo$data$yOffset > 8){
            badyOffset <- append(badyOffset, child_geo$data$locationName)
          }
          # Check if alphaOrientation is plausible. Should be 0 degrees for PAR.
          if(child_geo$data$alphaOrientation > 0){
            badAlpha <- append(badAlpha, child_geo$data$locationName)
          }
          # Check if beta orientation is plausible. Should be 0 degrees for PAR.
          if(child_geo$data$betaOrientation != 0){
            badBeta <- append(badBeta, child_geo$data$locationName)
          }
          # Check if gammaOrientation is plausible. Should be 0 degrees for PAR.
          if(child_geo$data$gammaOrientation > 0){
            badGamma <- append(badGamma, child_geo$data$locationName)
          }
          # Check is height is plausible
          if(child_geo$data$zOffset != 0){
            # Don't add sensor to "badSensorDepths" if it's already in "nonexistentSensors"
            if(sum(grepl(pattern = child_geo$data$locationName, x = nonexistentSensors)) == 0){
              badSensorDepths <- append(badSensorDepths, child_geo$data$locationName)
              badSensorDepths_depth <- append(badSensorDepths_depth, child_geo$data$zOffset)
              badSensorDepths_site <- append(badSensorDepths_site, child_geo$data$siteCode)
              badSensorDepths_sensor <- append(badSensorDepths_sensor, sensorName)
            }
          }
            
          
        }else if(grepl("Soil IR", child_geo$data$locationDescription)){
          sensorName <- "IR"
          # Check if height is plausible (0.1 to 0.5 m). Wider range of tolerance than Net Rad since sensor height is dictated by getting Net Rad to meet its height requirement.
          if(child_geo$data$zOffset > 0.5 | child_geo$data$zOffset < 0.1){
            # Don't add sensor to "badSensorDepths" if it's already in "nonexistentSensors"
            if(sum(grepl(pattern = child_geo$data$locationName, x = nonexistentSensors)) == 0){
              badSensorDepths <- append(badSensorDepths, child_geo$data$locationName)
              badSensorDepths_depth <- append(badSensorDepths_depth, child_geo$data$zOffset)
              badSensorDepths_site <- append(badSensorDepths_site, child_geo$data$siteCode)
              badSensorDepths_sensor <- append(badSensorDepths_sensor, sensorName)
            }
          }
          # Check if xOffset is plausible (-1 m to 8 m)
          if(child_geo$data$xOffset < -1 | child_geo$data$xOffset > 8){
            badxOffset <- append(badxOffset, child_geo$data$locationName)
          }
          # Check if yOffset is plausible (-1 m to 8 m)
          if(child_geo$data$yOffset < -1 | child_geo$data$yOffset > 8){
            badyOffset <- append(badyOffset, child_geo$data$locationName)
          }
          # Check if alphaOrientation is plausible. Should be -60 to -75 degrees relative to horizontal (i.e., pointing down) for IR.
          if(child_geo$data$alphaOrientation < -75 | child_geo$data$alphaOrientation > -60){
            badAlpha <- append(badAlpha, child_geo$data$locationName)
          }
          # Check if beta orientation is plausible. Should be 0 degrees for IR.
          if(child_geo$data$betaOrientation != 0){
            badBeta <- append(badBeta, child_geo$data$locationName)
          }
          # Check if gammaOrientation is plausible. Should be >90 and <270 degrees azimuth for IR.
          if(child_geo$data$gammaOrientation < 90 | child_geo$data$gammaOrientation > 270){
            badGamma <- append(badGamma, child_geo$data$locationName)
          }
          
          
        }else if(grepl("Net Rad", child_geo$data$locationDescription)){
          sensorName <- "NR"
          # Check if height is plausible (0.25 to 0.3 m)
          if(child_geo$data$zOffset > 0.3 | child_geo$data$zOffset < 0.2){
            # Don't add sensor to "badSensorDepths" if it's already in "nonexistentSensors"
            if(sum(grepl(pattern = child_geo$data$locationName, x = nonexistentSensors)) == 0){
              badSensorDepths <- append(badSensorDepths, child_geo$data$locationName)
              badSensorDepths_depth <- append(badSensorDepths_depth, child_geo$data$zOffset)
              badSensorDepths_site <- append(badSensorDepths_site, child_geo$data$siteCode)
              badSensorDepths_sensor <- append(badSensorDepths_sensor, sensorName)
            }
          }
          # Check if xOffset is plausible (-1 m to 8 m)
          if(child_geo$data$xOffset < -1 | child_geo$data$xOffset > 8){
            badxOffset <- append(badxOffset, child_geo$data$locationName)
          }
          # Check if yOffset is plausible (-1 m to 8 m)
          if(child_geo$data$yOffset < -1 | child_geo$data$yOffset > 8){
            badyOffset <- append(badyOffset, child_geo$data$locationName)
          }
          # Check if alphaOrientation is plausible. Should be 0 degrees for NR.
          if(child_geo$data$alphaOrientation > 0){
            badAlpha <- append(badAlpha, child_geo$data$locationName)
          }
          # Check if beta orientation is plausible. Should be 0 degrees for NR.
          if(child_geo$data$betaOrientation != 0){
            badBeta <- append(badBeta, child_geo$data$locationName)
          }
          # Check if gammaOrientation is plausible. Should be 0 degrees for NR.
          if(child_geo$data$gammaOrientation >0){
            badGamma <- append(badGamma, child_geo$data$locationName)
          }
          
          
        }else if(grepl("Relative", child_geo$data$locationDescription)){
          sensorName <- "RH"
          # Check if xOffset is plausible (-3 m to 10 m)
          if(child_geo$data$xOffset < -3 | child_geo$data$xOffset > 10){
            badxOffset <- append(badxOffset, child_geo$data$locationName)
          }
          # Check if yOffset is plausible (-3 m to 10 m)
          if(child_geo$data$yOffset < -3 | child_geo$data$yOffset > 10){
            badyOffset <- append(badyOffset, child_geo$data$locationName)
          }
          # Check if alphaOrientation is plausible. Should be 0 degrees for RH.
          if(child_geo$data$alphaOrientation > 0){
            badAlpha <- append(badAlpha, child_geo$data$locationName)
          }
          # Check if beta orientation is plausible. Should be 0 degrees for RH.
          if(child_geo$data$betaOrientation != 0){
            badBeta <- append(badBeta, child_geo$data$locationName)
          }
          # Check if gammaOrientation is plausible. Should be 0 degrees for RH.
          if(child_geo$data$gammaOrientation >0){
            badGamma <- append(badGamma, child_geo$data$locationName)
          }
          # Check is height is plausible
          if(child_geo$data$zOffset < 0.01 | child_geo$data$zOffset > 2){
            # Don't add sensor to "badSensorDepths" if it's already in "nonexistentSensors"
            if(sum(grepl(pattern = child_geo$data$locationName, x = nonexistentSensors)) == 0){
              badSensorDepths <- append(badSensorDepths, child_geo$data$locationName)
              badSensorDepths_depth <- append(badSensorDepths_depth, child_geo$data$zOffset)
              badSensorDepths_site <- append(badSensorDepths_site, child_geo$data$siteCode)
              badSensorDepths_sensor <- append(badSensorDepths_sensor, sensorName)
            }
          }
          
          
        }else if(grepl("CO2", child_geo$data$locationDescription)){
          sensorName <- paste0("C_", substr(child_geo$data$locationDescription, nchar(child_geo$data$locationDescription)-1, nchar(child_geo$data$locationDescription)))
          co2SensorName <- append(co2SensorName, sensorName)
          co2SensorDepths <- append(co2SensorDepths, child_geo$data$zOffset)
          # Check if depth is plausible (0 to -0.5 m)
          if(child_geo$data$zOffset > 0 | child_geo$data$zOffset < -0.5){
            # Don't add sensor to "badSensorDepths" if it's already in "nonexistentSensors"
            if(sum(grepl(pattern = child_geo$data$locationName, x = nonexistentSensors)) == 0){
              badSensorDepths <- append(badSensorDepths, child_geo$data$locationName)
              badSensorDepths_depth <- append(badSensorDepths_depth, child_geo$data$zOffset)
              badSensorDepths_site <- append(badSensorDepths_site, child_geo$data$siteCode)
              badSensorDepths_sensor <- append(badSensorDepths_sensor, sensorName)
            }
          }
          # Check if xOffset is plausible (-1 m to 8 m)
          if(child_geo$data$xOffset < -1 | child_geo$data$xOffset > 8){
            badxOffset <- append(badxOffset, child_geo$data$locationName)
          }
          # Check if yOffset is plausible (-1 m to 8 m)
          if(child_geo$data$yOffset < -1 | child_geo$data$yOffset > 8){
            badyOffset <- append(badyOffset, child_geo$data$locationName)
          }
          # Check if alphaOrientation is plausible. Should be 0 to 30 degrees from vertical for CO2.
          if(child_geo$data$alphaOrientation < 0 | child_geo$data$alphaOrientation > 30){
            badAlpha <- append(badAlpha, child_geo$data$locationName)
          }
          # Check if beta orientation is plausible. Should be 0 degrees for CO2.
          if(child_geo$data$betaOrientation != 0){
            badBeta <- append(badBeta, child_geo$data$locationName)
          }
          # Check if gammaOrientation is plausible. Should be 0 to 360 degrees for CO2.
          if(child_geo$data$gammaOrientation < 0 | child_geo$data$gammaOrientation > 360){
            badGamma <- append(badGamma, child_geo$data$locationName)
          }
          
          
        }else if(grepl("Heat", child_geo$data$locationDescription)){
          sensorName <- "HF"
          # Check if depth is plausible (-0.06 to -0.1 m)
          if(child_geo$data$zOffset > -0.06 | child_geo$data$zOffset < -0.1){
            # Don't add sensor to "badSensorDepths" if it's already in "nonexistentSensors"
            if(sum(grepl(pattern = child_geo$data$locationName, x = nonexistentSensors)) == 0){
              badSensorDepths <- append(badSensorDepths, child_geo$data$locationName)
              badSensorDepths_depth <- append(badSensorDepths_depth, child_geo$data$zOffset)
              badSensorDepths_site <- append(badSensorDepths_site, child_geo$data$siteCode)
              badSensorDepths_sensor <- append(badSensorDepths_sensor, sensorName)
            }
          }
          # Check if xOffset is plausible (-1 m to 8 m)
          if(child_geo$data$xOffset < -1 | child_geo$data$xOffset > 8){
            badxOffset <- append(badxOffset, child_geo$data$locationName)
          }
          # Check if yOffset is plausible (-1 m to 8 m)
          if(child_geo$data$yOffset < -1 | child_geo$data$yOffset > 8){
            badyOffset <- append(badyOffset, child_geo$data$locationName)
          }
          # Check if alphaOrientation is plausible. Should be 0 to 30 degrees from horizontal for HF.
          if(child_geo$data$alphaOrientation < 0 | child_geo$data$alphaOrientation > 30){
            badAlpha <- append(badAlpha, child_geo$data$locationName)
          }
          # Check if beta orientation is plausible. Should be 0 degrees for HF.
          if(child_geo$data$betaOrientation != 0){
            badBeta <- append(badBeta, child_geo$data$locationName)
          }
          # Check if gammaOrientation is plausible. Should be 0 to 360 degrees for HF.
          if(child_geo$data$gammaOrientation < 0 | child_geo$data$gammaOrientation > 360){
            badGamma <- append(badGamma, child_geo$data$locationName)
          }
          
          
        }else if(grepl("Soil Water", child_geo$data$locationDescription)){
          sensorName <- "SWC"
          # Check if xOffset is plausible (-1 m to 8 m)
          if(child_geo$data$xOffset < -1 | child_geo$data$xOffset > 8){
            badxOffset <- append(badxOffset, child_geo$data$locationName)
          }
          # Check if yOffset is plausible (-1 m to 8 m)
          if(child_geo$data$yOffset < -1 | child_geo$data$yOffset > 8){
            badyOffset <- append(badyOffset, child_geo$data$locationName)
          }
          # Check if alphaOrientation is plausible. Should be 0 to 5 degrees from vertical for SWC.
          if(child_geo$data$alphaOrientation < 0 | child_geo$data$alphaOrientation > 5){
            badAlpha <- append(badAlpha, child_geo$data$locationName)
          }
          # Check if beta orientation is plausible. Should be 0 degrees for SWC.
          if(child_geo$data$betaOrientation != 0){
            badBeta <- append(badBeta, child_geo$data$locationName)
          }
          # Check if gammaOrientation is plausible. Should be 0 to 360 degrees for SWC.
          if(child_geo$data$gammaOrientation < 0 | child_geo$data$gammaOrientation > 360){
            badGamma <- append(badGamma, child_geo$data$locationName)
          }
          # Check if depth of sensor reference point is plausible (0.02 to -0.02 m)
          if(child_geo$data$zOffset > 0.02 | child_geo$data$zOffset < -0.02){
            # Don't add sensor to "badSensorDepths" if it's already in "nonexistentSensors"
            if(sum(grepl(pattern = child_geo$data$locationName, x = nonexistentSensors)) == 0){
              badSensorDepths <- append(badSensorDepths, child_geo$data$locationName)
              badSensorDepths_depth <- append(badSensorDepths_depth, child_geo$data$zOffset)
              badSensorDepths_site <- append(badSensorDepths_site, child_geo$data$siteCode)
              badSensorDepths_sensor <- append(badSensorDepths_sensor, sensorName)
            }
          }
          
          
        }else if(grepl("Throughfall", child_geo$data$locationDescription)){
          sensorName <- "TF"
          # Check if xOffset is plausible (-1 m to 8 m)
          if(child_geo$data$xOffset < -1 | child_geo$data$xOffset > 8){
            badxOffset <- append(badxOffset, child_geo$data$locationName)
          }
          # Check if yOffset is plausible (-1 m to 8 m)
          if(child_geo$data$yOffset < -1 | child_geo$data$yOffset > 8){
            badyOffset <- append(badyOffset, child_geo$data$locationName)
          }
          # Check if alphaOrientation is plausible. Should be 0 degrees for TF.
          if(child_geo$data$alphaOrientation < 0 | child_geo$data$alphaOrientation > 5){
            badAlpha <- append(badAlpha, child_geo$data$locationName)
          }
          # Check if beta orientation is plausible. Should be 0 degrees for TF.
          if(child_geo$data$betaOrientation != 0){
            badBeta <- append(badBeta, child_geo$data$locationName)
          }
          # Check if gammaOrientation is plausible. Should be 0 degrees for TF.
          if(child_geo$data$gammaOrientation > 0){
            badGamma <- append(badGamma, child_geo$data$locationName)
          }
          # Check if height of sensor collection area is plausible (0.3 to 0.6 m)
          if(child_geo$data$zOffset > 0.6 | child_geo$data$zOffset < 0.3){
            # Don't add sensor to "badSensorDepths" if it's already in "nonexistentSensors"
            if(sum(grepl(pattern = child_geo$data$locationName, x = nonexistentSensors)) == 0){
              badSensorDepths <- append(badSensorDepths, child_geo$data$locationName)
              badSensorDepths_depth <- append(badSensorDepths_depth, child_geo$data$zOffset)
              badSensorDepths_site <- append(badSensorDepths_site, child_geo$data$siteCode)
              badSensorDepths_sensor <- append(badSensorDepths_sensor, sensorName)
            }
          }
        }
        
        
        # We assume the soil plot is flat in the following caluclations. Although this is not necessarily true of all soil plots, the small size the plots (nominally 5 x 5 m) means the uncertainty caused by this assumption will be small (e.g., up to tens of centimeters).
        
        ## CALCULATE SENSOR LOCATION BASED ON LINE PERPENDICULAR TO C1 TO C4
        # Calculate the geolocation (utm) of the point on C1 to C4 that is perpendicular to the sensor (i.e, xOffset from C1 toward C4)
        northing_intersect_C14 <- ((dist_intersect_C14 / dist_C14) * (utm["C4", "northing"] - utm["C1", "northing"])) + utm["C1", "northing"]
        easting_intersect_C14 <- ((dist_intersect_C14 / dist_C14) * (utm["C4", "easting"] - utm["C1", "easting"])) + utm["C1", "easting"]
        #points(easting_intersect_C14, northing_intersect_C14, col="blue")
        
        # Calculate sensor northing and easting based on a line perpendicular to C1 to C4.
        # Using pythagorus, z_C14^2 + (grad_perpen_C14 * z_C14)^2 = dist_intersect_C12^2
        # where z_C14 is the distance (m) between (easting_intersect_C14, northing_intersect_C14) and a point where an easting line passing thorugh (easting_intersect_C14, northing_intersect_C14) and a northing line passing through the sensor location intersect.
        # If we solve this equation for z_C14, we can add z_C14 to easting_intersect_C14 to get the easting of the sensor location, and add (grad_perpen_C14 * z_C14) to northing_intersect_C14 to get the northing of the sensor location
        sign_14 <- 1
        if(dist_intersect_C12 < 0){sign_14 <- -1}
        z_C14 <- (sqrt((dist_intersect_C12^2) / (1 + grad_perpen_C14^2))) * sign_14
        if(utm["C1", "northing"] < utm["C4", "northing"] && 
           utm["C1", "northing"] < utm["C2", "northing"] && 
           utm["C1", "easting"] < utm["C4", "easting"] && 
           utm["C1", "easting"] > utm["C2", "easting"] | 
           utm["C1", "northing"] > utm["C4", "northing"] && 
           utm["C1", "northing"] < utm["C2", "northing"] && 
           utm["C1", "easting"] > utm["C4", "easting"] && 
           utm["C1", "easting"] > utm["C2", "easting"] ){
          easting_sensor_C14 <- easting_intersect_C14 - z_C14
          northing_sensor_C14 <- northing_intersect_C14 + (abs(grad_perpen_C14) * z_C14)
        }else if(utm["C1", "northing"] > utm["C4", "northing"] && 
                 utm["C1", "northing"] > utm["C2", "northing"] && 
                 utm["C1", "easting"] > utm["C4", "easting"] && 
                 utm["C1", "easting"] < utm["C2", "easting"] | 
                 utm["C1", "northing"] < utm["C4", "northing"] && 
                 utm["C1", "northing"] > utm["C2", "northing"] && 
                 utm["C1", "easting"] < utm["C4", "easting"] && 
                 utm["C1", "easting"] < utm["C2", "easting"] ){
          easting_sensor_C14 <- easting_intersect_C14 + z_C14
          northing_sensor_C14 <- northing_intersect_C14 - (abs(grad_perpen_C14) * z_C14)
        }else if(utm["C1", "northing"] > utm["C4", "northing"] && 
                 utm["C1", "northing"] < utm["C2", "northing"] && 
                 utm["C1", "easting"] < utm["C4", "easting"] && 
                 utm["C1", "easting"] < utm["C2", "easting"] | 
                 utm["C1", "northing"] < utm["C4", "northing"] && 
                 utm["C1", "northing"] < utm["C2", "northing"] && 
                 utm["C1", "easting"] > utm["C4", "easting"] && 
                 utm["C1", "easting"] < utm["C2", "easting"] ){
          easting_sensor_C14 <- easting_intersect_C14 + z_C14
          northing_sensor_C14 <- northing_intersect_C14 + (abs(grad_perpen_C14) * z_C14)
        }else if(utm["C1", "northing"] < utm["C4", "northing"] && 
                 utm["C1", "northing"] > utm["C2", "northing"] && 
                 utm["C1", "easting"] > utm["C4", "easting"] && 
                 utm["C1", "easting"] > utm["C2", "easting"] | 
                 utm["C1", "northing"] > utm["C4", "northing"] && 
                 utm["C1", "northing"] > utm["C2", "northing"] && 
                 utm["C1", "easting"] < utm["C4", "easting"] && 
                 utm["C1", "easting"] > utm["C2", "easting"] ){
          easting_sensor_C14 <- easting_intersect_C14 - z_C14
          northing_sensor_C14 <- northing_intersect_C14 - (abs(grad_perpen_C14) * z_C14)
        }
        #points(easting_sensor_C14, northing_sensor_C14, col="blue")
        ## END: CALCULATE SENSOR LOCATION BASED ON LINE PERPENDICULAR TO C1 TO C4
        
        
        ## CALCULATE SENSOR LOCATION BASED ON LINE PERPENDICULAR TO C1 TO C2
        # Calculate the geolocation (utm) of the point on C1 to C2 that is perpendicular to the sensor (i.e, yOffset from C1 toward C2)
        northing_intersect_C12 <- ((dist_intersect_C12 / dist_C12) * (utm["C2", "northing"] - utm["C1", "northing"])) + utm["C1", "northing"]
        easting_intersect_C12 <- ((dist_intersect_C12 / dist_C12) * (utm["C2", "easting"] - utm["C1", "easting"])) + utm["C1", "easting"]
        #points(easting_intersect_C12, northing_intersect_C12, col="red")
        
        # Calculate sensor northing and easting based on a line perpendicular to C1 to C2.
        # Using pythagorus, z_C12^2 + (grad_perpen_C12 * z_C12)^2 = dist_intersect_C14^2
        # where z is the distance (m) between (easting_intersect_C12, northing_intersect_C12) and a point where an easting line passing thorugh (easting_intersect_C12, northing_intersect_C12) and a northing line passing through the sensor location intersect.
        # If we solve this equation for z_C12, we can add z_C12 to easting_intersect_C12 to get the easting of the sensor location, and add (grad_perpen_C12 * z_C12) to northing_intersect_C12 to get the northing of the sensor location
        sign_12 <- 1
        if(dist_intersect_C14 < 0){sign_12 <- -1}
        z_C12 <- (sqrt((dist_intersect_C14^2) / (1 + grad_perpen_C12^2))) * sign_12
        if(utm["C1", "northing"] < utm["C4", "northing"] && 
           utm["C1", "northing"] < utm["C2", "northing"] && 
           utm["C1", "easting"] < utm["C4", "easting"] && 
           utm["C1", "easting"] > utm["C2", "easting"] | 
           utm["C1", "northing"] < utm["C4", "northing"] && 
           utm["C1", "northing"] > utm["C2", "northing"] && 
           utm["C1", "easting"] < utm["C4", "easting"] && 
           utm["C1", "easting"] < utm["C2", "easting"] ){
          easting_sensor_C12 <- easting_intersect_C12 + z_C12
          northing_sensor_C12 <- northing_intersect_C12 + (abs(grad_perpen_C12) * z_C12)
        }else if(utm["C1", "northing"] > utm["C4", "northing"] && 
                 utm["C1", "northing"] > utm["C2", "northing"] && 
                 utm["C1", "easting"] > utm["C4", "easting"] && 
                 utm["C1", "easting"] < utm["C2", "easting"] | 
                 utm["C1", "northing"] > utm["C4", "northing"] && 
                 utm["C1", "northing"] < utm["C2", "northing"] && 
                 utm["C1", "easting"] > utm["C4", "easting"] && 
                 utm["C1", "easting"] > utm["C2", "easting"] ){
          easting_sensor_C12 <- easting_intersect_C12 - z_C12
          northing_sensor_C12 <- northing_intersect_C12 - (abs(grad_perpen_C12) * z_C12)
        }else if(utm["C1", "northing"] > utm["C4", "northing"] && 
                 utm["C1", "northing"] < utm["C2", "northing"] && 
                 utm["C1", "easting"] < utm["C4", "easting"] && 
                 utm["C1", "easting"] < utm["C2", "easting"] | 
                 utm["C1", "northing"] > utm["C4", "northing"] && 
                 utm["C1", "northing"] > utm["C2", "northing"] && 
                 utm["C1", "easting"] < utm["C4", "easting"] && 
                 utm["C1", "easting"] > utm["C2", "easting"] ){
          easting_sensor_C12 <- easting_intersect_C12 + z_C12
          northing_sensor_C12 <- northing_intersect_C12 - (abs(grad_perpen_C12) * z_C12)
        }else if(utm["C1", "northing"] < utm["C4", "northing"] && 
                 utm["C1", "northing"] > utm["C2", "northing"] && 
                 utm["C1", "easting"] > utm["C4", "easting"] && 
                 utm["C1", "easting"] > utm["C2", "easting"] | 
                 utm["C1", "northing"] < utm["C4", "northing"] && 
                 utm["C1", "northing"] < utm["C2", "northing"] && 
                 utm["C1", "easting"] > utm["C4", "easting"] && 
                 utm["C1", "easting"] < utm["C2", "easting"] ){
          easting_sensor_C12 <- easting_intersect_C12 - z_C12
          northing_sensor_C12 <- northing_intersect_C12 + (abs(grad_perpen_C12) * z_C12)
        }
        #points(easting_sensor_C12, northing_sensor_C12, col="red")
        ## END: CALCULATE SENSOR LOCATION BASED ON LINE PERPENDICULAR TO C1 TO C2
        
        
        # For temperature and CO2 sensors, update the northing and easting to account for installation angle (alphaOrientation)
        if(grepl(pattern = "T_", x=sensorName) | grepl(pattern = "C_", x=sensorName)){
          # No need to do this adjustment if installed vertically
          if(child_geo$data$alphaOrientation != 0){
            
            # Calculate the distance from the actual sensor location from the soil surface where sensor enters the soil
            # angles converted from degrees to radians units (i.e., multiply by pi and divide by 180)
            sensorOffset <- sin((child_geo$data$alphaOrientation) * pi / 180) * abs(child_geo$data$zOffset)
            
            # Assembly azimuth (gammaOrientation) is measured on the top surface of the assembly, from the highest point towards the lowest point. Therefore, an azimuth of 90 degrees (east) means that the soil sensors are offset from the x and y location towards 270 degrees (west).
            
            # Calculate easting and northing offset offset
            if(child_geo$data$gammaOrientation >= 0 && child_geo$data$gammaOrientation <= 90){
              refAngle <- 90
              # angles converted from degrees to radians units (i.e., multiply by pi and divide by 180)
              sensorOffsetEasting <- cos((refAngle - child_geo$data$gammaOrientation) * pi / 180) * sensorOffset
              sensorOffsetNorthing <- sin((refAngle - child_geo$data$gammaOrientation) * pi / 180) * sensorOffset
              
              # Adjust easting and northing calculated based on line perpendicular to C1 to C4
              easting_sensor_C14 <- easting_sensor_C14 - sensorOffsetEasting
              northing_sensor_C14 <- northing_sensor_C14 - sensorOffsetNorthing
              
              # Adjust easting and northing calculated based on line perpendicular to C1 to C2
              easting_sensor_C12 <- easting_sensor_C12 - sensorOffsetEasting
              northing_sensor_C12 <- northing_sensor_C12 - sensorOffsetNorthing
            }else if(child_geo$data$gammaOrientation > 90 && child_geo$data$gammaOrientation <= 180){
              refAngle <- 90
              # angles converted from degrees to radians units (i.e., multiply by pi and divide by 180)
              sensorOffsetEasting <- cos((child_geo$data$gammaOrientation - refAngle) * pi / 180) * sensorOffset
              sensorOffsetNorthing <- sin((child_geo$data$gammaOrientation - refAngle) * pi / 180) * sensorOffset
              
              # Adjust easting and northing calculated based on line perpendicular to C1 to C4
              easting_sensor_C14 <- easting_sensor_C14 - sensorOffsetEasting
              northing_sensor_C14 <- northing_sensor_C14 + sensorOffsetNorthing
              
              # Adjust easting and northing calculated based on line perpendicular to C1 to C2
              easting_sensor_C12 <- easting_sensor_C12 - sensorOffsetEasting
              northing_sensor_C12 <- northing_sensor_C12 + sensorOffsetNorthing
            }else if(child_geo$data$gammaOrientation > 180 && child_geo$data$gammaOrientation <= 270){
              refAngle <- 180
              # angles converted from degrees to radians units (i.e., multiply by pi and divide by 180)
              sensorOffsetEasting <- cos((child_geo$data$gammaOrientation - refAngle) * pi / 180) * sensorOffset
              sensorOffsetNorthing <- sin((child_geo$data$gammaOrientation - refAngle) * pi / 180) * sensorOffset
              
              # Adjust easting and northing calculated based on line perpendicular to C1 to C4
              easting_sensor_C14 <- easting_sensor_C14 + sensorOffsetEasting
              northing_sensor_C14 <- northing_sensor_C14 + sensorOffsetNorthing
              
              # Adjust easting and northing calculated based on line perpendicular to C1 to C2
              easting_sensor_C12 <- easting_sensor_C12 + sensorOffsetEasting
              northing_sensor_C12 <- northing_sensor_C12 + sensorOffsetNorthing
            }else if(child_geo$data$gammaOrientation > 270 && child_geo$data$gammaOrientation <= 360){
              refAngle <- 180
              # angles converted from degrees to radians units (i.e., multiply by pi and divide by 180)
              sensorOffsetEasting <- cos((refAngle - child_geo$data$gammaOrientation) * pi / 180) * sensorOffset
              sensorOffsetNorthing <- sin((refAngle - child_geo$data$gammaOrientation) * pi / 180) * sensorOffset
              
              # Adjust easting and northing calculated based on line perpendicular to C1 to C4
              easting_sensor_C14 <- easting_sensor_C14 + sensorOffsetEasting
              northing_sensor_C14 <- northing_sensor_C14 - sensorOffsetNorthing
              
              # Adjust easting and northing calculated based on line perpendicular to C1 to C2
              easting_sensor_C12 <- easting_sensor_C12 + sensorOffsetEasting
              northing_sensor_C12 <- northing_sensor_C12 - sensorOffsetNorthing
            } 
          }
        }
        
        
        # Calculate mean and standard deviation of sensor location based on lines perpendicular to C1 to C4 and C1 to C2
        mean_easting_sensor <- mean(c(easting_sensor_C14, easting_sensor_C12))
        sd_easting_sensor <- sd(c(easting_sensor_C14, easting_sensor_C12))
        mean_northing_sensor <- mean(c(northing_sensor_C14, northing_sensor_C12))
        sd_northing_sensor <- sd(c(northing_sensor_C14, northing_sensor_C12))
        
        # Add mean sensor location to plot with oval showing 1 SD around that location
        points(mean_easting_sensor, mean_northing_sensor, type="n")
        draw_ellipse(mean_easting_sensor, mean_northing_sensor, sd_easting_sensor*2, sd_northing_sensor*2)
        text(mean_easting_sensor, mean_northing_sensor,label=sensorName, cex=0.5)

        # Add north arrow to plot
        addnortharrow(scale = 0.25)
        
        # Save geolocation data to data frame
        x <- c(sensorName, mean_easting_sensor, sd_easting_sensor, mean_northing_sensor, sd_northing_sensor)
        sensorGeo[i, "sensorName"] <- sensorName
        sensorGeo[i, "mean_easting_sensor"] <- mean_easting_sensor
        sensorGeo[i, "mean_northing_sensor"] <- mean_northing_sensor
        sensorGeo[i, "utm_zone_sensor"] <- plot_geo$data$locationPolygon$coordinates$utmZone[1]
        sensorGeo[i, "utm_hemisphere_sensor"] <- plot_geo$data$locationPolygon$coordinates$utmHemisphere[1]
        sensorGeo[i, "sd_easting_sensor"] <- sd_easting_sensor
        sensorGeo[i, "sd_northing_sensor"] <- sd_northing_sensor
        sensorGeo[i, "height_sensor"] <- child_geo$data$zOffset
        sensorGeo[i, "alpha_sensor"] <- child_geo$data$alphaOrientation
        sensorGeo[i, "beta_sensor"] <- child_geo$data$betaOrientation
        sensorGeo[i, "gamma_sensor"] <- child_geo$data$gammaOrientation
        sensorGeo[i, "mean_plot_elevation"] <- mean_plot_elevation
        sensorGeo[i, "sd_plot_elevation"] <- sd_plot_elevation
        sensorGeo[i, "domain"] <- plot_geo$data$domainCode
        sensorGeo[i, "site"] <- plot_geo$data$siteCode
        sensorGeo[i, "soilPlot"] <- plotID
        sensorGeo[i, "plotLocationName"] <- plot_geo$data$locationName
        sensorGeo[i, "sensorLocationName"] <- child_geo$data$locationName
        
        # Reset offset parameters to prevent carryover from one sensor to the next if anything weird happens
        child_geo <- NA
        #dist_intersect_C14 <- NA
        #dist_intersect_C12 <- NA
      }
    }
    # Add legend to plot
    legend("topleft", legend=c("T_Z#...Temperature", "SWC....Moisture", "C_D#...CO2 conc.", "HF......Heat flux", "TF......Throughfall", "PAR.....Ground-level PAR", "NR......Net radiation", "IR......Infrared temp.", "RH......Relative humidity", "T_NA....Combined temp and moisture"), cex=0.4, bty="n")
    legend("bottomleft", legend="±2SD", pch=16, col=rgb(1, 0, 0, 0.1), bty="n", bg=rgb(1, 0, 0, 0.1), pt.cex=1.3, cex=0.5)
    
    # turn of graphics
    dev.off()
    
    # convert utm to lat/long and add to data frame
    sensorGeo <- sensorGeo[complete.cases(sensorGeo), ]
    spatialInfo <- paste0("+proj=utm +zone=", sensorGeo$utm_zone_sensor[1], sensorGeo$utm_hemisphere_sensor[1], " +datum=WGS84")
    sputm <- SpatialPoints(sensorGeo[c("mean_easting_sensor", "mean_northing_sensor")], proj4string=CRS(spatialInfo))
    spgeo <- spTransform(sputm, CRS("+proj=longlat +datum=WGS84"))
    sensorGeo$Latitude <- spgeo$mean_northing_sensor
    sensorGeo$Longitude <- spgeo$mean_easting_sensor
    
    # Save the sensor geolocation data frame. Remove rows that only contain NAs
    sensorGeo <- sensorGeo[order(sensorGeo$sensorName), ]
    filename2 <- paste0(graph_name, ".csv")
    write.csv(sensorGeo, filename2, row.names = F)
    
    # Create a plot of soil temperature sensor depths
    tempPlotName <- paste0("tempDepths_", filename1)
    png(tempPlotName, width=800, height=800, units="px", res=150)
    plot(1:length(tempSensorDepths), tempSensorDepths, ylab="Depth (m)", ylim=c(T_depthThreshold, 0), xlim=c(0,11), xlab="Random index", main=tempPlotName)
    text(tempSensorDepths, labels = tempSensorName, pos = 4)
    dev.off()
    tempSensorDepths <- c()
    tempSensorName <- c()
    
    # Create a plot of soil CO2 sensor sensor depths
    if(length(co2SensorDepths) > 0){ # Only do if plot includes CO2 sensors
      co2PlotName <- paste0("co2Depths_", filename1)
      png(co2PlotName, width=800, height=800, units="px", res=150)
      plot(1:length(co2SensorDepths), co2SensorDepths, ylab="Depth (m)", ylim=c(-0.5, 0), xlim=c(0,5), xlab="Random index", main=co2PlotName)
      text(co2SensorDepths, labels = co2SensorName, pos = 4)
      dev.off()
      co2SensorDepths <- c()
      co2SensorName <- c()
    }
    
    # Clear the plot information
    plot_geo <- NA
    mean_plot_elevation <- NA
    sd_plot_elevation <- NA
    utm <- NA
    sensor_geolocs <- NA
  }, error=function(e){cat("ERROR :",conditionMessage(e), "\n", SP_geoloc[j] )})
}
# Save list of geolocation urls with problems (e.g., missing utm data, incorrect units, etc)
write.csv(urlsMissingUTMdata, "Problems/urlsMissingUTMdata.csv", row.names=F)
badSensorDepths <- cbind(badSensorDepths, badSensorDepths_depth, badSensorDepths_site, badSensorDepths_sensor)
write.csv(badSensorDepths, "Problems/badSensorDepths.csv", row.names=F)
write.csv(badUTMvalues, "Problems/badUTMvalues.csv", row.names=F)
write.csv(nonexistentSensors, "Problems/nonexistentSensors.csv", row.names=F)
write.csv(possibleBadOrientation, "Problems/possibleBadOrientation.csv", row.names=F)
badPlotElevation <- cbind(badPlotElevation, badPlotElevation_site, badPlotElevation_minElev, badPlotElevation_maxElev, badPlotElevation_towerElev)
write.csv(badPlotElevation, "Problems/badPlotElevation.csv", row.names=F)
write.csv(badxOffset, "Problems/badxOffset.csv", row.names=F)
write.csv(badyOffset, "Problems/badyOffset.csv", row.names=F)
write.csv(badAlpha, "Problems/badAlpha.csv", row.names=F)
write.csv(badBeta, "Problems/badBeta.csv", row.names=F)
write.csv(badGamma, "Problems/badGamma.csv", row.names=F)

# Print number of errors
nrow(badSensorDepths)-1
length(badxOffset)-1
length(badyOffset)-1
length(badAlpha)-1
length(badBeta)-1
length(badGamma)-1
length(possibleBadOrientation)-1
length(urlsMissingUTMdata)-1
length(badUTMvalues)-1
nrow(badPlotElevation)-1
length(nonexistentSensors)-1
# Total number of potential errors
sum(nrow(badSensorDepths)-1, 
    length(badxOffset)-1, 
    length(badyOffset)-1, 
    length(badAlpha)-1, 
    length(badBeta)-1, 
    length(badGamma)-1, 
    length(possibleBadOrientation)-1, 
    length(urlsMissingUTMdata)-1, 
    length(badUTMvalues)-1, 
    nrow(badPlotElevation)-1, 
    length(nonexistentSensors)-1)



# read in all sensor geolocation files
files <- list.files(pattern=".csv")
files <- files[!grepl("sensorGeo_All.csv", files)]
sensorGeo <- read.csv(files[1], stringsAsFactors = F)
for(a in 2:length(files)){
  sensorGeo <- rbind(sensorGeo, read.csv(files[a]))
}
plot(sensorGeo$Longitude, sensorGeo$Latitude)
plot(sensorGeo$sd_easting_sensor, sensorGeo$sd_northing_sensor)
plot(sensorGeo$height_sensor, sensorGeo$mean_plot_elevation)
plot(sensorGeo$alpha_sensor, sensorGeo$gamma_sensor)
plot(sensorGeo$mean_plot_elevation, sensorGeo$sd_plot_elevation)

# Save a csv file with all sensor geolocation data for all sites
write.csv(sensorGeo, "sensorGeo_All.csv", row.names=F)

mean(sensorGeo$alpha_sensor[grep("T_Z", sensorGeo$sensorName)])
sd(sensorGeo$alpha_sensor[grep("T_Z", sensorGeo$sensorName)])

# Create boxplot of CO2 sensor depths by measurement level
boxplot(sensorGeo$height_sensor[grep("C_ 1", sensorGeo$sensorName)], sensorGeo$height_sensor[grep("C_ 2", sensorGeo$sensorName)], sensorGeo$height_sensor[grep("C_ 3", sensorGeo$sensorName)], notch=T, range = 1)

# Create boxplot of soil temperature sensor depths by measurement level
boxplot(sensorGeo$height_sensor[grep("T_Z1", sensorGeo$sensorName)], 
        sensorGeo$height_sensor[grep("T_Z2", sensorGeo$sensorName)], 
        sensorGeo$height_sensor[grep("T_Z3", sensorGeo$sensorName)], 
        sensorGeo$height_sensor[grep("T_Z4", sensorGeo$sensorName)], 
        sensorGeo$height_sensor[grep("T_Z5", sensorGeo$sensorName)], 
        sensorGeo$height_sensor[grep("T_Z6", sensorGeo$sensorName)], 
        sensorGeo$height_sensor[grep("T_Z7", sensorGeo$sensorName)], 
        sensorGeo$height_sensor[grep("T_Z8", sensorGeo$sensorName)], 
        sensorGeo$height_sensor[grep("T_Z9", sensorGeo$sensorName)], notch=T, range = 1)


sensorGeo$height_sensor[intersect(grep("T_", sensorGeo$sensorName), which(sensorGeo$height_sensor > -0.01))]

# Determine distance between certain sensors
tempSWCdist <- c()
rowSWC <- c()
rowTemp <- c()
rowHF <- c()
tempHFdist <- c()
SWCHFdist <- c()
c1C2dist <- c()
c1C3dist <- c()
c2C3dist <- c()
for(i in 1:length(unique(sensorGeo$plotLocationName))){
  # Distance between temp and SWC sensors
  rowSWC <- intersect(grep("SWC", sensorGeo$sensorName), grep(unique(sensorGeo$plotLocationName)[i], sensorGeo$plotLocationName))
  rowTemp <- intersect(grep("T_Z1", sensorGeo$sensorName), grep(unique(sensorGeo$plotLocationName)[i], sensorGeo$plotLocationName))
  rowHF <- intersect(grep("HF", sensorGeo$sensorName), grep(unique(sensorGeo$plotLocationName)[i], sensorGeo$plotLocationName))
  rowTempSWC <- intersect(grep("T_NA", sensorGeo$sensorName), grep(unique(sensorGeo$plotLocationName)[i], sensorGeo$plotLocationName))[1] # identify if combined temperature and moisture sensor is present (currently just PUUM)
  
  if(length(rowTemp) + length(rowSWC) == 2){ # only do if seperate soil temp and moisture sensors are present in the same plot
    tempSWCdist[i] <- sqrt((sensorGeo$mean_easting_sensor[rowTemp] - sensorGeo$mean_easting_sensor[rowSWC])^2 + (sensorGeo$mean_northing_sensor[rowTemp] - sensorGeo$mean_northing_sensor[rowSWC])^2)
  }else if(!is.na(rowTempSWC)){ # do if a combined soil temp and moisture sensor is present (currently just at PUUM)
    tempSWCdist[i] <- 0
  }

  if(length(rowHF) + length(rowTemp) == 2){
    tempHFdist[i] <- sqrt((sensorGeo$mean_easting_sensor[rowTemp] - sensorGeo$mean_easting_sensor[rowHF])^2 + (sensorGeo$mean_northing_sensor[rowTemp] - sensorGeo$mean_northing_sensor[rowHF])^2)
  }
  if(length(rowHF) + length(rowSWC) == 2){
    SWCHFdist[i] <- sqrt((sensorGeo$mean_easting_sensor[rowHF] - sensorGeo$mean_easting_sensor[rowSWC])^2 + (sensorGeo$mean_northing_sensor[rowHF] - sensorGeo$mean_northing_sensor[rowSWC])^2)
  }
  if( !is.na(rowTempSWC) && length(rowHF)>0){
    tempHFdist[i] <- sqrt((sensorGeo$mean_easting_sensor[rowTempSWC] - sensorGeo$mean_easting_sensor[rowHF])^2 + (sensorGeo$mean_northing_sensor[rowTempSWC] - sensorGeo$mean_northing_sensor[rowHF])^2)
    SWCHFdist[i] <- sqrt((sensorGeo$mean_easting_sensor[rowTempSWC] - sensorGeo$mean_easting_sensor[rowHF])^2 + (sensorGeo$mean_northing_sensor[rowTempSWC] - sensorGeo$mean_northing_sensor[rowHF])^2)
  }
  
  # Distance between CO2 sensors
  rowC1 <- intersect(grep("C_ 1", sensorGeo$sensorName), grep(unique(sensorGeo$plotLocationName)[i], sensorGeo$plotLocationName))
  rowC2 <- intersect(grep("C_ 2", sensorGeo$sensorName), grep(unique(sensorGeo$plotLocationName)[i], sensorGeo$plotLocationName))
  rowC3 <- intersect(grep("C_ 3", sensorGeo$sensorName), grep(unique(sensorGeo$plotLocationName)[i], sensorGeo$plotLocationName))
  if(length(rowC1)+length(rowC1)+length(rowC1) >1){ # Only do this if at least two CO2 sensors are present in the plot
    c1C2dist[i] <- sqrt((sensorGeo$mean_easting_sensor[rowC1] - sensorGeo$mean_easting_sensor[rowC2])^2 + (sensorGeo$mean_northing_sensor[rowC1] - sensorGeo$mean_northing_sensor[rowC2])^2)
    c1C3dist[i] <- sqrt((sensorGeo$mean_easting_sensor[rowC1] - sensorGeo$mean_easting_sensor[rowC3])^2 + (sensorGeo$mean_northing_sensor[rowC1] - sensorGeo$mean_northing_sensor[rowC3])^2)
    c2C3dist[i] <- sqrt((sensorGeo$mean_easting_sensor[rowC2] - sensorGeo$mean_easting_sensor[rowC3])^2 + (sensorGeo$mean_northing_sensor[rowC2] - sensorGeo$mean_northing_sensor[rowC3])^2)
  }
}

boxplot(tempSWCdist, tempHFdist, SWCHFdist, names = c("Temp-SWC", "Temp-HF", "SWC-HF"), ylab="Distance (m)")
boxplot(c1C2dist, c1C3dist, c2C3dist, names = c("C1-C2", "C1-C3", "C2-C3"), ylab="Distance (m)")

