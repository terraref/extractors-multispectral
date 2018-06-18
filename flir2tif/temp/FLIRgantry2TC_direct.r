#Convert raw FLIR data to brightness temperatures
#apply corrections using ambient environmental temperature
#direct conversion from DN to temperature method

# FLIRgantry2TC_direct.r input.bin output.dat output.hdr
args = commandArgs(trailingOnly=TRUE)

# test if there is 2 arguments: if not, return an error
if (length(args)<3) {
  stop("At least one argument must be supplied (input file).n", call.=FALSE)
} else {
  fFLIRfil = args[1]
  foFLIRfil = args[2]
  fohdrFLIRfil = args[3]
}

#specify the raw FLIR camera data file name
# FLIRdir  <- "C:/Data/FLIRcalibrate2018/FLIR_CalRoom_2-12-18/2-12-2018/Room_35C/30CwinN/"
# FLIRfil  <- "00000045.raw"
# fFLIRfil <- paste(FLIRdir,FLIRfil,sep="")

#create output file name
# len_FLIRfil <- nchar(FLIRfil)
#assume standard 3 character extension
# oFLIRfil  <- paste(substr(FLIRfil,1,len_FLIRfil-4),"_brightTC_directmethod.dat",sep="")
# foFLIRfil <- paste(FLIRdir,oFLIRfil,sep="")
print(paste("output brightness temperature file to be named:",foFLIRfil))

#create hdr file to go with the output binary file
# len_oFLIRfil <- nchar(oFLIRfil)
# ohdrFLIRfil  <- paste(substr(oFLIRfil,1,len_oFLIRfil-3),"hdr",sep="")
# fohdrFLIRfil <- paste(FLIRdir,ohdrFLIRfil,sep="")
print(paste("output brightness temperature header file to be named:",fohdrFLIRfil))

#create a descriptor to document the image in the header file
descrip <- "Test Data Feb 2018"

#FLIR dimensions
FLIRnumpixels <- 640
FLIRnumlines  <- 480
FLIRtotpix    <- FLIRnumpixels*FLIRnumlines

#read the ambient temperature Celsius
#NEED TO POINT to data source for ambient temperature value

#tc0 is the ambient temperature in Celsius
#DN0 is the FLIR digital number at tc0
#L0 is the spectral radiance value for the FLIR camera at tc0
tc0 <- 25.0 ##set value for now, revise when path to ambient temperature values known

#temperature - spectral radiance table file
tkLdir     <- "/home/extractor/temp/"
tkLfil     <- "LSTC2Lradn10to110.csv"
ftkLfil    <- paste(tkLdir,tkLfil,sep="")
tkLdat     <- read.table(ftkLfil,header=TRUE,sep=",",skip=4)
tkLhdrrecs <- readLines(ftkLfil,n=3) #spectral response function information
tkLfunctionshape                      <- tkLhdrrecs[1]
tkLunits                              <- tkLhdrrecs[2]
tkLresponsefunctioncornersresponses   <- tkLhdrrecs[3]
tkLresponsefunctioncornerswavelengths <- tkLhdrrecs[4]
#variable names for tkLdat dataframe: TC, L

#begin function definitions

#convert ambient temperature C into FLIR DN value quadratic function fit
# using data at 5,15,25,35,40,45C
flirtcamb2DN <- function(tcamb) {
  bcoefs <- c(10387.0745396,121.746624749,0.760502417804)
  DNamb  <- bcoefs[1] + (bcoefs[2]*tcamb) + (bcoefs[3]*tcamb^2)
  return(DNamb)
}

#create interpolation functions to convert between temperature and spectral radiance
# using the estimated spectral response function for the FLIR camera and window
tc2Lf <- approxfun(tkLdat$TC,tkLdat$L,method="linear",rule=1)
L2tcf <- approxfun(tkLdat$L,tkLdat$TC,method="linear",rule=1)

#find value of slope correction term given ambient temperature
Lslopef <- function(tcamb) {
  dcoefs <- c(1.088989456566198,-0.000184781721912)
  #N.B.- coefficients meaningful to 4 signficant figures but 
  # including 12 digits to allow traceability of calibration
  Lslope <- dcoefs[1] + dcoefs[2]*tcamb
  return(Lslope)
}

DN2tcf <- function(DN,cubeparms) {
  #return temperature values given a set of 4 cubic polynomial coefficients
  #cubeparms are ambient temperature dependent
  tc <- cubeparms[1]+cubeparms[2]*DN+cubeparms[3]*DN^2+cubeparms[4]*DN^3
  return(tc)
}
DN2tcLegf <- function(DN,cubeparms) {
  #return temperature values given Legendre polynomial coefficients
  tc <- cubeparms[1]+cubeparms[2]*DN+cubeparms[3]*0.5*(3*DN^2-1)+cubeparms[4]*0.5*(5*DN^3-3*DN)
  return(tc)
}

#function to return numtcamb values of target temperatures using cubic polynomial

DN2tcvecf <- function(DNvec,cubeparmmat,tcambvec) {
  numDN <- length(DNvec)
  dimcubeparmmat <- dim(cubeparmmat)
  numamb <- dimcubeparmmat[1]
  len_tcamb <- length(tcambvec)
  otcmat <- matrix(0.0,nrow=numDN,ncol=numamb)
  for(dnct in 1:numDN) {
    curDN <- DNvec[dnct]  
    for(tcct in 1:numamb) {
      ltcval <- DN2tcf(curDN,cubeparmmat[tcct,])
      otcmat[dnct,tcct] <- ltcval
    }
  }
  #otcfr <- data.frame(otcmat)
  
  return(otcmat)
}

#end function definitions

#cubic function coefficients and associated ambient temperature in Celsius
numtcamb <- 6
cubeLparmmat <- matrix(0.0,nrow=numtcamb,ncol=4)
tcambvec <- c(5,15,25,35,40,45)
#Legendre polynomial coefficients
cubeLparmmat[1,] <- c(41.11111, 537.2295, -40.93659, 5.992838)
cubeLparmmat[2,] <- c(39.25000, 552.2500, -37.58235, 4.074052)
cubeLparmmat[3,] <- c(40.18868, 823.8618, -60.33062, 5.800347)
cubeLparmmat[4,] <- c(43.23529, 497.6501, -35.07224, 4.818448)
cubeLparmmat[5,] <- c(43.23529, 497.5725, -36.20323, 4.342843)
cubeLparmmat[6,] <- c(43.23529, 497.7198, -34.09575, 4.363040)

#conventional nls regression derived coefficients
cubeparmmat[1,] <- c(-138.3187,0.01872808,-6.206219e-7,9.303883e-12)
cubeparmmat[2,] <- c(-126.0155,0.01619146,-4.659710e-7,6.282221e-12)
cubeparmmat[3,] <- c(-123.7946,0.01550752,-4.207259e-7,5.358188e-12)
cubeparmmat[4,] <- c(-135.0648,0.01743997,-5.381814e-7,7.680099e-12)
cubeparmmat[5,] <- c(-135.6012,0.01719204,-5.115752e-7,7.001709e-12)
cubeparmmat[6,] <- c(-134.4436,0.01696944,-5.052935e-7,7.029535e-12)

#compute ambient DN and L
#DN0    <- flirtcamb2DN(tc0) #quadratic function fit, calibration range 5 to 45C
#L0     <- tc2Lf(tc0) #linear interpolation function, computation range -5 to 110C
#Lslope <- Lslopef(tc0) #linear function fit to calibration room data
# room temperatures 5,15,25,35,40,45C

#read the FLIR data
flircon   <- file(fFLIRfil,"rb")
FLIRDNdat <- readBin(flircon,integer(),n=FLIRtotpix,size=2,signed=FALSE,endian="little")
close(flircon)

numvalsFLIR <- length(FLIRDNdat)
FLIRtc  <- rep(0.0,numvalsFLIR)
for(fct in 1:numvalsFLIR) {
  curDN <- as.numeric(FLIRDNdat[fct])
  ltcvec <- DN2tcvecf(curDN,cubeparmmat,tcambvec)
  #print(paste("ltcvec:",ltcvec))
  tcapprox <- approx(tcambvec,ltcvec,tc0,method="linear",rule=1)
  FLIRtc[fct] <- tcapprox$y
  #print(paste("FLIRDN:",curDN,"FLIRtc:",FLIRtc))
}

#compute the differences between DN0 and observed image data DN
#delDN <- as.numeric(FLIRDNdat)-DN0

#compute the estimated difference in spectral radiance
#delL <- Lslope*delDN

#compute the estimated total spectral radiances
#FLIRLdat <- L0 + delL #L0 a scalar, delL a vector containing FLIRtotpix values

#compute the estimated brightness temperatures
#FLIRtc <- L2tcf(FLIRLdat) #using linear interpolation function

#write the brightness temperature file as binary float
focon <- file(foFLIRfil,"wb")
writeBin(FLIRtc,focon,size=4,endian="little")
close(focon)
print(paste("wrote brightness temperature file:",foFLIRfil))

#write the header file to assist reading the output binary file
hdr1 <- paste("ENVIR description = {",descrip,"}",sep="")
numpixstr <- sprintf("%4d",FLIRnumpixels)
numlinstr <- sprintf("%4d",FLIRnumlines)
hdr2 <- paste("samples = ",numpixstr,sep="")
hdr3 <- paste("lines = ",numlinstr,sep="")
hdr4 <- paste("bands = 1")
hdr5 <- paste("header offset = 0")
hdr6 <- paste("file type = ENVI Standard")
hdr7 <- paste("data type = 4") #N.B.- type 4 is float, 12 is unsigned short int
hdr8 <- paste("interleave = bsq")
hdr9 <- paste("sensor type = Unknown")
hdr10 <- paste("byte order = 0") #N.B.- zero is little-endian 

fohdrcon <- file(fohdrFLIRfil,"w")
write(hdr1,fohdrcon,append=FALSE)
write(hdr2,fohdrcon,append=TRUE)
write(hdr3,fohdrcon,append=TRUE)
write(hdr4,fohdrcon,append=TRUE)
write(hdr5,fohdrcon,append=TRUE)
write(hdr6,fohdrcon,append=TRUE)
write(hdr7,fohdrcon,append=TRUE)
write(hdr8,fohdrcon,append=TRUE)
write(hdr9,fohdrcon,append=TRUE)
write(hdr10,fohdrcon,append=TRUE)
close(fohdrcon)
print(paste("wrote header file:",fohdrFLIRfil))
