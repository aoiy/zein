import pandas as pd
from datetime import datetime

inputs = {
    "orderList": [
        {
            "orderId": "000001",
            "customerName": "上海电气",
            "orderDate": "2021-03-01",
            "arrivalDate": "2021-03-03",
            "workType": {
                "installation": True,
                "adjustment": True,
                "inspection": False,
                "troubleshooting": False,
                "otherwork": False
            },
            "deviceModel": "FM5一体式无旁路",
            "deviceNumber": 1,
            "firstDevice": False,
            "specialModel": False,
            "expectDays": 2.0,
            "customerProvince": "浙江省",
            "customerCity": "杭州市",
            "exclusiveStaff": ["李四", ],
        },
    ],
    "staffList": [
        {
            "staffId": "0001",
            "staffName": "张三",
            "schedule": {
                "2021-03-01": {
                    "workStatus": "公司",
                    "orderId": ""
                },
                "2021-03-02": {
                    "workStatus": "公司",
                    "orderId": ""
                },
                "2021-03-03": {
                    "workStatus": "公司",
                    "orderId": ""
                },
            },
            "ability": {
                "FM5": True,
                "VM5": True,
                "FM6": False,
                "VM6": False,
            },
            "staffProvince": "浙江省",
            "staffCity": "杭州市",
            "totalDealtDevices": 20,
            "totalErrandDays": 10.0,
            "totalErrandTimes": 5,

        }
    ],
    "config": {
        "includeAbility": False,
        "schedulePeriod": 30,
        "responseTimeLimit": 3,
        "deviceNumWeight": 0.2,
        "errandDaysWeight": 0.7,
        "errandTimesWeight": 0.1,
        "deviceThreshold": 4,
        "compTimeLimit": 3,
        "compTimeDays": 2,
    },
}


class LaborDispatch:
    def __init__(self, input_dict):
        self.order_list = input_dict['orderList']
        self.staff_list = input_dict['staffList']
        self.configs = input_dict['config']
        self.order = None
        self.staff = None
        self.calender = None
        self.__preprocess()

    def __preprocess(self):
        self.order = pd.DataFrame(self.order_list)
        self.order = self.order.join(pd.DataFrame(self.order['workType'].values.tolist())).drop(
            'workType', axis=1)
        self.order.set_index('orderId', inplace=True)
        self.order[['orderDate', 'arrivalDate']] = self.order[['orderDate', 'arrivalDate']].apply(pd.to_datetime)
        self.order['expectEndDate'] = \
            [self.order.loc[i, 'arrivalDate'] + pd.Timedelta(self.order.loc[i, 'expectDays'], unit='D')
             for i in self.order.index]

        self.staff = pd.DataFrame(self.staff_list)
        self.staff = self.staff.join(pd.DataFrame(self.staff['ability'].values.tolist())).drop(
            'ability', axis=1)
        schedule = self.staff['schedule'].values.tolist()
        schedule_reform = {(date, key): [value] for sche in schedule
                           for date, content in sche.items() for key, value in content.items()}
        self.staff.columns = pd.MultiIndex.from_product([self.staff.columns, ['']])
        self.staff = self.staff.join(pd.DataFrame(schedule_reform)).drop('schedule', axis=1, level=0)
        self.staff.set_index('staffId', inplace=True)

        errand_days_sum = self.staff.totalErrandDays.sum()
        errand_times_sum = self.staff.totalErrandTimes.sum()
        dealt_device_sum = self.staff.totalDealtDevices.sum()
        self.staff['workloadIndex'] = \
            self.staff.totalErrandDays / errand_days_sum * self.configs['errandDaysWeight'] \
            + self.staff.totalDealtDevices / dealt_device_sum * self.configs['deviceNumWeight'] \
            + self.staff.totalErrandTimes / errand_times_sum * self.configs['errandTimesWeight']

        start_date = datetime.now().date()
        end_date = self.order.expectEndDate.max().date()
        date_range = pd.date_range(start_date, end_date)
        self.calender = pd.DataFrame(index=self.order.index, columns=date_range)
        for i in self.calender.index:
            self.calender.loc[i, self.order.loc[i, 'arrivalDate']: self.order.loc[i, 'expectEndDate']] = 1
        self.calender.fillna(0, inplace=True)
        self.calender.columns = [date.date().isoformat() for date in self.calender.columns]

    def get_available_staff(self, order_ss):
        """
        return available staff for a certain work order, considering constrains:
        (1) arrival date, (2) exclusive staff
        """
        if (order_ss.arrivalDate - order_ss.orderDate).days < self.configs['responseTimeLimit']:
            arrival_dates = []
            for date_priority in range(1, self.configs['responseTimeLimit']):
                if order_ss.troubleshooting:
                    arrival_date = order_ss.orderDate + pd.Timedelta(date_priority, unit='D')
                else:
                    arrival_date = order_ss.orderDate \
                                   + pd.Timedelta(self.configs['responseTimeLimit'] - date_priority, unit='D')
                arrival_dates.append((arrival_date, date_priority))
        else:
            arrival_dates = [(order_ss.arrivalDate, 1)]
        arrival_dates = [(date.date().isoformat(), priority) for date, priority in arrival_dates]

        available_staff = pd.DataFrame()
        for date, priority in arrival_dates:
            conditions = (self.staff[date].workStatus == '公司') & \
                         (~self.staff.staffName.isin(order_ss.exclusiveStaff))
            avl_staff = self.staff[conditions]
            avl_staff['datePriorty'] = priority
            avl_staff.loc[:, (date, 'orderId')] = order_ss.name
            available_staff = available_staff.append(avl_staff)

        return available_staff

    def get_prior_staff(self, order_ss):
        """
        return the prior person from all available staff, considering rules:
        (1) workload balance, (2) todo: nearby previous order
        """
        sent_num = 2 if order_ss.firstDevice or order_ss.deviceNumber >= self.configs['deviceThreshold'] else 1
        avl_staff = self.get_available_staff(order_ss)

        # required ability accurate to generic device model, e.g. FM5一体式无旁路 -> FM5
        device_model = order_ss.deviceModel[:3]
        avl_staff['priorityIndex'] = 0

        if len(avl_staff) < sent_num:
            # todo: nearby dispatch
            print(f'Not enough staff for order {order_ss.orderId}')
            return avl_staff
        avl_staff.sort_values('workloadIndex', ascending=False, inplace=True)
        return avl_staff[:sent_num]

    def cal_priority(self, target_order, free_staff):
        priority_matrix = pd.DataFrame(index=target_order, columns=free_staff)
        return priority_matrix

    def get_match(self, weight_matrix):
        return 0

    def dispatch(self):
        cur_calender = self.calender.copy()
        date_most_order = cur_calender.sum().idxmax()
        date_order = cur_calender[date_most_order]
        target_order = date_order[date_order == 1].index.tolist()

        date_staff = self.staff[date_most_order]
        free_staff = date_staff[date_staff['workStatus'] == '公司'].index.tolist()

        priority_matrix =self.cal_priority(target_order, free_staff)
        match = self.get_match(priority_matrix)

        # matching = {order_id: None for order_id in self.order.index}
        # for order_id in self.order.index:
        #     order_ss = self.order.loc[order_id, :]
        #     prior_staff = self.get_prior_staff(order_ss)
        return free_staff


if __name__ == '__main__':
    model = LaborDispatch(inputs)
    res = model.dispatch()
