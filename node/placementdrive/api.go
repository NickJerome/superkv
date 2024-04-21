package placementdrive

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
)

type PDController struct {
	pd *PlacementDriver
	r  *gin.Engine
}

func NewPDController(pd *PlacementDriver, r *gin.Engine) *PDController {
	return &PDController{
		pd: pd,
		r:  r,
	}
}

func (pdc *PDController) Start() {
	pd := pdc.r.Group("/pd")
	{
		pd.POST("/join", pdc.JoinPD)
	}
	region := pdc.r.Group("/region")
	{
		region.POST("/start", pdc.StartRegion)
		region.POST("/create", pdc.CreateRegion)
		region.GET("/members", pdc.GetMemberShip)
	}
	key := pdc.r.Group("/key")
	{
		key.GET("/locate", pdc.LocateRegionByKey)
	}
}

func (pdc *PDController) JoinPD(c *gin.Context) {
	nid, err := strconv.ParseUint(c.PostForm("nid"), 10, 64)
	if err != nil {
		c.String(http.StatusBadRequest, "Param nid isn't a valid value")
		return
	}

	addr := c.PostForm("addr")

	err = pdc.pd.Join(nid, addr)
	if err != nil {
		c.String(http.StatusBadRequest, "nid or addr isn't a valid value, error - %v", err.Error())
		return
	}

	c.String(http.StatusOK, "join success")
}

func (pdc *PDController) CreateRegion(c *gin.Context) {
	rid, err := pdc.pd.CreateRegion()
	if err != nil {
		c.String(http.StatusServiceUnavailable, err.Error())
		return
	}

	c.String(http.StatusOK, strconv.FormatUint(rid, 10))
}

func (pdc *PDController) StartRegion(c *gin.Context) {
	rid, err := strconv.ParseUint(c.PostForm("rid"), 10, 64)
	if err != nil {
		c.String(http.StatusBadRequest, "Param rid isn't a valid value")
		return
	}

	nid, err := strconv.ParseUint(c.PostForm("nid"), 10, 64)
	if err != nil {
		c.String(http.StatusBadRequest, "Param nid isn't a valid value")
		return
	}

	err = pdc.pd.StartRegion(rid, nid, nil)
	if err != nil {
		c.String(http.StatusBadRequest, "start region fail, error: %v", err.Error())
		return
	}

	c.String(http.StatusOK, "success create region %d", rid)
}

func (pdc *PDController) GetMemberShip(c *gin.Context) {
	rid, err := strconv.ParseUint(c.Query("rid"), 10, 64)
	if err != nil {
		c.String(http.StatusBadRequest, "Param rid isn't a valid value")
		return
	}

	members, err := pdc.pd.GetMemberShip(rid)
	if err != nil {
		c.String(http.StatusBadRequest, "get membership fail, error: %v", err.Error())
		return
	}

	c.JSON(http.StatusOK, members)
}

func (pdc *PDController) LocateRegionByKey(c *gin.Context) {
	key := c.Query("key")

	rid, err := pdc.pd.LocateRegionByKey(key)
	if err != nil {
		c.String(http.StatusBadRequest, "Locate regionID fail, error: %v", err.Error())
		return
	}

	c.JSON(http.StatusOK, rid)
}
