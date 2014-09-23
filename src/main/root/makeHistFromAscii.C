#include <fstream>

#include <TH1D.h>
#include <TFile.h>
#include <TNtuple.h>
#include <TPad.h>

#include "Riostream.h"

void makeHistFromAscii(const char *fileName, TH1D *hist, bool saveTex) {

	/* 
		Read double data from a 2-column tab-separated ASCII file,
		make a Root file with a histogram and an n-tuple (TTree),
		draw the histogram, and save it to a .tex file.
		
		Note: the data filename (up to extension which atm is expected 
		to be .dat) will be used for saving .root and .tex files.
	
		Usage:
	
		root [#] TH1D *hist = new TH1D("TH1D","Some description",100,-1.,1.5) ;
		root [#} .x makeHistFromAscii("npmi.dat",hist)

	**/

	ifstream in ;
	in.open(fileName) ;

	TString genericFileName = fileName ;
	genericFileName.ReplaceAll(".dat","") ;

	TFile *f = new TFile(Form("%s.root",genericFileName.Data()),"recreate") ;
	
	Double_t x,y ;
	Int_t nlines = 0 ;
	TNtuple *ntuple = new TNtuple("ntuple","data from ascii file","x:y") ;
	
	while (1) {
		in >> x >> y ;
		if (!in.good()) break ;
		if (nlines < 5) printf("x=%8f, y=%8f\n",x,y) ;
		hist->Fill(y) ;
		ntuple->Fill(x,y) ;
		nlines++ ;
	}
	printf(" found %d points\n",nlines) ;

	// gStyle->SetPaperSize(10.,10.) ;

 	// write hist to current dir ( which is the TFile )
	hist->Write("hist") ;
	hist->Draw() ;

	if (saveTex) {
		// save histogram to .tex file
		gPad->Print(Form("%s.tex",genericFileName.Data())) ;
	}
	
	in.close() ;
	f->Write() ;

	// disown TFile of an object for further processing after macro
	// hist->SetDirectory(0) ;
	// explicitly delete that object once you're done with it
	// delete hist ;

	f->Close() ;
	delete f ;
}
