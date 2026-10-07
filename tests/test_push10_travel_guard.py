# push 10 review (2026-10-07): the travel validator must not retire a whole host on a blip.
import validate_travel_urls as v


def test_guard_holds_a_host_whose_dead_share_is_too_high():
    host_of = {i: "www.amnhealthcare.com" for i in range(1, 2001)}
    host_of.update({i: "www.vivian.com" for i in range(2001, 4001)})
    bad = list(range(1, 1201)) + [2001, 2002, 2003]
    retire, held = v._guard_mass_retire(bad, host_of)
    assert held == {"www.amnhealthcare.com": 1200}
    assert retire == [2001, 2002, 2003]


def test_guard_lets_normal_expiry_through():
    host_of = {i: "www.vivian.com" for i in range(1, 10001)}
    bad = list(range(1, 801))
    retire, held = v._guard_mass_retire(bad, host_of)
    assert held == {}
    assert len(retire) == 800


def test_guard_needs_both_the_share_and_the_minimum():
    host_of = {i: "nomadhealth.com" for i in range(1, 1001)}
    bad = list(range(1, 401))
    retire, held = v._guard_mass_retire(bad, host_of)
    assert held == {} and len(retire) == 400
